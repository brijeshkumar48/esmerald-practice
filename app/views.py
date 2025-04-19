import os
from typing import Any, Dict, Optional
from bson import ObjectId
from esmerald import Form, Query, Response, get, post, delete, Request, status
from app.commonDao import CommonDAO
from app.models.file_model import Student, UploadedMediaFile
from app.models.stuDao import StudentDAO
from app.utils import generate_response

UPLOAD_DIR = "media/uploads/files"


@post("/upload-file")
async def upload_file(
    logger: Any,
    request: Request,
    data: Any = Form(),
    **kwargs: Any,
) -> Response:
    """Handle file upload."""
    uploaded_file = data.get("path")
    file_type = data.get("type")

    os.makedirs(UPLOAD_DIR, exist_ok=True)

    # Save file
    file_path = os.path.join(UPLOAD_DIR, uploaded_file.filename)
    with open(file_path, "wb") as buffer:
        buffer.write(await uploaded_file.read())

    relative_path = os.path.normpath(file_path).replace(os.sep, "/")

    data = {
        "path": relative_path,
        "type": file_type,
        "name": str(uploaded_file.filename),
    }

    upload_instance = await UploadedMediaFile.objects.create(**data)

    return generate_response(
        request=request,
        data=data,
        message="Ok",
    )


@delete("/{upload_file_id:str}", status_code=status.HTTP_200_OK)
async def delete_file(request: Request, upload_file_id: str) -> Response:
    try:
        instance = await UploadedMediaFile.objects.get(id=ObjectId(upload_file_id))
        print("==================", instance)
        # Uncomment this if you want to delete the file
        # await instance.delete()

        return generate_response(
            request=request,
            data={},
            message="File deleted successfully"
        )
    except Exception as e:
        return generate_response(
            request=request,
            data={},
            message=f"Error: {str(e)}"
        )


@get("/student")
async def stu_details(
    request: Request, 
    q: Optional[Dict[str, Any]] = Query(default=None), 
    **kwargs: Any
    ) -> Response:
    """Get student details."""
    student_dao = StudentDAO(db="test_database")

    # stu = await student_dao.get_all()

    stu = await student_dao.search(
    params=q,
    projection=[
            "name",
            "std",
            ("address.state", "state"),
            ("address.pincode", "pincode"),
            ("school_id.name", "school_name"),
            ("school_id.board", "school_board"),
        ]
    )

    return generate_response(
        request=request,
        data=stu,
        message="Ok",
    )

@post("/create_stu")
async def insert_student(
    request: Request, 
    **kwargs: Any
    ) -> Response:
    student_data = {
        "name": "John Doe",
        "std": "10th",
        "school_id": ObjectId("60c72b2f9b1e8b8b8b8b8b9b")
    }

    # Insert the student document into the collection
    new_student = await Student.objects.create(**student_data)

    return generate_response(
        request=request,
        data={},
        message="Ok",
    )
