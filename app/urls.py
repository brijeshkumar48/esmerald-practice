# urls.py
from esmerald.routing.router import Gateway, Include

from app.views import (
    create_student,
    delete_file,
    insert_student,
    llm_response,
    stu_details,
    upload_file,
)

urlpatterns = [
    Gateway("/api/file", handler=upload_file),
    Gateway("/api/file/delete-file/", handler=delete_file),
    Gateway("/api/stu", handler=stu_details),
    Gateway("/api/stu", handler=insert_student),
    Gateway("/api/stu", handler=create_student),
    Gateway("/api/llm", handler=llm_response),
]
