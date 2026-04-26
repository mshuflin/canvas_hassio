"""Canvas Hub."""
from __future__ import annotations

import logging
import asyncio
import itertools

from canvas_parent_api import Canvas
from canvas_parent_api.models.assignment import Assignment
from canvas_parent_api.models.course import Course
from canvas_parent_api.models.observee import Observee
from canvas_parent_api.models.submission import Submission

from homeassistant import config_entries
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import CONF_BASEURI, CONF_SECRET, DEFAULT_SEMAPHORE, DOMAIN, SCAN_INT, CONF_SEMAPHORE

_LOGGER = logging.getLogger(__name__)


class CanvasHub(DataUpdateCoordinator):
    """Canvas Hub definition."""

    config_entry: config_entries.ConfigEntry

    def __init__(self, hass: HomeAssistant, entry: config_entries.ConfigEntry) -> None:
        """Initialize."""
        super().__init__(hass, _LOGGER, name=DOMAIN, update_interval=SCAN_INT)
        self.config_entry = entry

        self._baseuri = entry.data[CONF_BASEURI].rstrip("/")
        self._secret = entry.data[CONF_SECRET]
        self._client = Canvas(f"{self._baseuri}", f"{self._secret}")
        self._semaphore = asyncio.Semaphore(entry.options.get(CONF_SEMAPHORE, DEFAULT_SEMAPHORE))
        self._session = async_get_clientsession(hass)

    async def _async_update_data(self):
        """Update data via library and build Master Payload."""
        try:
            _LOGGER.debug("Starting Canvas data update and master payload construction")
            
            # 1. Get Observees (Students)
            students = await self.get_students()
            
            # 2. Get Courses for each student and build initial structure
            master_payload = {}
            all_courses = []
            
            for student in students:
                student_id = str(getattr(student, "id", ""))
                if not student_id:
                    continue
                    
                master_payload[student_id] = {
                    "student": student,
                    "courses": {}
                }
                
                try:
                    courseresp = await self.get_courses(student.id, self._semaphore)
                    for c_data in courseresp:
                        course = Course(c_data)
                        course_id = str(course.id)
                        all_courses.append(course)
                        
                        # Extract Grades
                        grades = {"score": None, "grade": None}
                        enrollments = getattr(course, 'enrollments', [])
                        if enrollments and len(enrollments) > 0:
                            enrollment = enrollments[0]
                            if isinstance(enrollment, dict):
                                grades["score"] = enrollments[0].get("computed_current_score")
                                grades["grade"] = enrollments[0].get("computed_current_grade")
                            else:
                                grades["score"] = getattr(enrollment, "computed_current_score", None)
                                grades["grade"] = getattr(enrollment, "computed_current_grade", None)
                        
                        master_payload[student_id]["courses"][course_id] = {
                            "course": course,
                            "grades": grades,
                            "categories": [],
                            "assignments": [],
                            "submissions": []
                        }
                except Exception as err:
                    _LOGGER.warning("Error fetching courses for student %s: %s", student_id, err)

            # 3. Fetch Assignments, Submissions, and Assignment Groups (Categories) in parallel
            assignment_tasks = []
            submission_tasks = []
            category_tasks = []
            task_info = [] # Store (student_id, course_id) for each task
            
            for student_id, student_data in master_payload.items():
                for course_id, course_data in student_data["courses"].items():
                    assignment_tasks.append(self.get_assignments(student_id, course_id, self._semaphore))
                    submission_tasks.append(self.get_submissions(student_id, course_id, self._semaphore))
                    category_tasks.append(self.get_assignment_groups(course_id, self._semaphore))
                    task_info.append((student_id, course_id))

            assignment_results = await asyncio.gather(*assignment_tasks, return_exceptions=True)
            submission_results = await asyncio.gather(*submission_tasks, return_exceptions=True)
            category_results = await asyncio.gather(*category_tasks, return_exceptions=True)

            all_assignments = []
            all_submissions = []

            for i, (student_id, course_id) in enumerate(task_info):
                # Process Assignments
                a_res = assignment_results[i]
                if not isinstance(a_res, Exception):
                    assignments = [Assignment(a) for a in a_res]
                    master_payload[student_id]["courses"][course_id]["assignments"] = assignments
                    all_assignments.extend(assignments)
                
                # Process Submissions
                s_res = submission_results[i]
                if not isinstance(s_res, Exception):
                    submissions = [Submission(s) for s in s_res]
                    master_payload[student_id]["courses"][course_id]["submissions"] = submissions
                    all_submissions.extend(submissions)
                
                # Process Categories
                c_res = category_results[i]
                if not isinstance(c_res, Exception):
                    master_payload[student_id]["courses"][course_id]["categories"] = c_res

            # Filter pending assignments for the flat list (compatibility)
            submitted_assignment_ids = set()
            for submission in all_submissions:
                if hasattr(submission, 'workflow_state') and hasattr(submission, 'assignment_id'):
                    workflow_state = getattr(submission, 'workflow_state', None)
                    submitted_at = getattr(submission, 'submitted_at', None)
                    grade = getattr(submission, 'grade', None)
                    score = getattr(submission, 'score', None)

                    if ((workflow_state == 'submitted' and submitted_at) or
                        (grade is not None and grade != '') or
                        (score is not None)):
                        assignment_id = str(getattr(submission, 'assignment_id', ''))
                        if assignment_id:
                            submitted_assignment_ids.add(assignment_id)

            pending_assignments = []
            for assignment in all_assignments:
                assignment_id = str(getattr(assignment, 'id', ''))
                if assignment_id and assignment_id not in submitted_assignment_ids:
                    pending_assignments.append(assignment)

            _LOGGER.debug("Finished Canvas data update: %s students processed in Master Payload", len(students))

            return {
                "master": master_payload,
                "students": students,
                "courses": all_courses,
                "assignments": all_assignments,
                "submissions": all_submissions,
                "pending_assignments": pending_assignments
            }

        except Exception as err:
            raise UpdateFailed(f"Error communicating with Canvas API: {err}") from err

    async def get_students(self):
        """Get handler for students."""
        return await self._client.observees()

    async def get_courses(self, student_id, sem):
        """Get handler for courses."""
        async with sem:
            return await self._client.courses(student_id)

    async def get_assignments(self, student_id, course_id, sem):
        """Get handler for assignments."""
        async with sem:
            return await self._client.assignments(student_id,course_id)

    async def get_submissions(self, student_id, course_id, sem):
        """Get handler for submissions with graceful error handling."""
        async with sem:
            try:
                return await self._client.submissions(student_id, course_id)
            except Exception as err:
                _LOGGER.warning("Access denied or error fetching submissions for student %s in course %s: %s", 
                               student_id, course_id, err)
                return []

    async def get_assignment_groups(self, course_id, sem):
        """Get assignment groups (categories) with graceful error handling."""
        async with sem:
            # Check if library has assignment_groups method
            if hasattr(self._client, "assignment_groups"):
                try:
                    return await self._client.assignment_groups(course_id)
                except Exception:
                    pass # Fallback to manual request
            
            # Fallback manual GET request
            url = f"{self._baseuri}/api/v1/courses/{course_id}/assignment_groups"
            headers = {"Authorization": f"Bearer {self._secret}"}
            try:
                async with self._session.get(url, headers=headers) as response:
                    if response.status in [401, 403]:
                        _LOGGER.warning("Access denied to assignment groups for course %s", course_id)
                        return []
                    response.raise_for_status()
                    return await response.json()
            except Exception as err:
                _LOGGER.warning("Error fetching assignment groups for course %s: %s", course_id, err)
                return []

    def poll_observees(self) -> list[dict]:
        """Get Canvas Observees (students)."""
        return self.data.get("students", [])

    def poll_courses(self) -> list[dict]:
        """Get Canvas Courses."""
        return self.data.get("courses", [])

    def poll_assignments(self) -> list[dict]:
        """Get Canvas Assignments."""
        return self.data.get("assignments", [])

    def poll_pending_assignments(self) -> list[dict]:
        """Get only pending (unsubmitted/ungraded) Canvas Assignments."""
        return self.data.get("pending_assignments", [])

    def poll_submissions(self) -> list[dict]:
        """Get Canvas Assignments."""
        return self.data.get("submissions", [])
