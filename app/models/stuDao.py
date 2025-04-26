from app.commonDao import CommonDAO
from app.models.app_models import Student


class StudentDAO(CommonDAO):
    model = Student
