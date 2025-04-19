from app.commonDao import CommonDAO
from app.models.file_model import Student


class StudentDAO(CommonDAO):
    model = Student