import asyncio
from bson import ObjectId
from app.models.file_model import Student

async def insert_student():
    student_data = {
        "name": "Brijesh",
        "std": "10th",
        "school_id": ObjectId("60c72b2f9b1e8b8b8b8b8b9b"),
        "address": {
            "village": "Maple Village",
            "state": "Maharashtra",
            "pincode": 400076
        }
    }

    # Insert the student document into the collection
    new_student = await Student.objects.create(**student_data)
    return new_student

# Run the coroutine
asyncio.run(insert_student())
