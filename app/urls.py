# urls.py
from esmerald.routing.router import Gateway, Include

from app.views import delete_file, insert_student, stu_details, upload_file

urlpatterns = [
    Gateway("/api/file", handler=upload_file),
    Gateway("/api/file/delete-file/", handler=delete_file),
    Gateway("/api/stu", handler=stu_details),
    Gateway("/api/stu", handler=insert_student),
]
