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

    async def _async_update_data(self):
        """Update data via library."""
        try:
            _LOGGER.debug("Starting Canvas data update")
            
            # 1. Get Observees (Students)
            students = await self.get_students()
            
            # 2. Get Courses for each student
            courses = []
            for student in students:
                try:
                    courseresp = await self.get_courses(student.id, self._semaphore)
                    courses.extend([Course(course) for course in courseresp])
                except Exception as err:
                    _LOGGER.warning("Error fetching courses for student %s: %s", student.id, err)

            # 3. Get Assignments and Submissions
            assignment_tasks = []
            submission_tasks = []
            
            for course in courses:
                observee = course.enrollments[0]
                if observee is not None:
                    student_id = observee.get("user_id", "")
                    if student_id:
                        assignment_tasks.append(self.get_assignments(student_id, course.id, self._semaphore))
                        submission_tasks.append(self.get_submissions(student_id, course.id, self._semaphore))

            assignment_results = await asyncio.gather(*assignment_tasks, return_exceptions=True)
            submission_results = await asyncio.gather(*submission_tasks, return_exceptions=True)

            assignments = []
            for res in assignment_results:
                if isinstance(res, Exception):
                    _LOGGER.warning("Error fetching assignments: %s", res)
                else:
                    assignments.extend([Assignment(a) for a in res])

            submissions = []
            for res in submission_results:
                if isinstance(res, Exception):
                    _LOGGER.warning("Error fetching submissions: %s", res)
                else:
                    submissions.extend([Submission(s) for s in res])

            # Filter pending assignments
            submitted_assignment_ids = set()
            for submission in submissions:
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
            for assignment in assignments:
                assignment_id = str(getattr(assignment, 'id', ''))
                if assignment_id and assignment_id not in submitted_assignment_ids:
                    pending_assignments.append(assignment)

            _LOGGER.debug("Finished Canvas data update: %s students, %s courses, %s assignments, %s submissions", 
                         len(students), len(courses), len(assignments), len(submissions))

            return {
                "students": students,
                "courses": courses,
                "assignments": assignments,
                "submissions": submissions,
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
