# urls.py
from esmerald.routing.router import Gateway, Include

from app.views import (
    create_student,
    delete_file,
    llm_response,
    protected_file,
    stu_details,
    upload_file,
)

urlpatterns = [
    Gateway("/api/stu", handler=stu_details),
    Gateway("/api/stu", handler=create_student),
    Gateway(
        path="/api/media/{file_path:path}",
        handler=protected_file,
    ),
    Gateway("/api/file", handler=upload_file),
    Gateway("/api/file/delete-file/", handler=delete_file),
    # Gateway("/api/stu", handler=insert_student),
    Gateway("/api/llm", handler=llm_response),
]

"""
# =============in correct===================
[
    {
        "$unwind": {
            "path": "$school_id.university_id.body",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$lookup": {
            "as": "school_id",
            "foreignField": "_id",
            "from": "schools",
            "localField": "school_id",
        }
    },
    {"$unwind": {"path": "$school_id", "preserveNullAndEmptyArrays": True}},
    {
        "$lookup": {
            "as": "school_id.university_id",
            "foreignField": "_id",
            "from": "universities",
            "localField": "school_id.university_id",
        }
    },
    {
        "$unwind": {
            "path": "$school_id.university_id",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$lookup": {
            "as": "school_id.university_id.body.country_id",
            "foreignField": "_id",
            "from": "countries",
            "localField": "school_id.university_id.body.country_id",
        }
    },
    {
        "$unwind": {
            "path": "$school_id.university_id.body.country_id",
            "preserveNullAndEmptyArrays": True,
        }
    },
]


# =============expected correct ==================
[
    {
        "$lookup": {
            "from": "schools",
            "localField": "school_id",
            "foreignField": "_id",
            "as": "school",
        }
    },
    {"$unwind": {"path": "$school", "preserveNullAndEmptyArrays": True}},
    {
        "$lookup": {
            "from": "universities",
            "localField": "school.university_id",
            "foreignField": "_id",
            "as": "university",
        }
    },
    {"$unwind": {"path": "$university", "preserveNullAndEmptyArrays": True}},
    {
        "$unwind": {
            "path": "$university.body",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$lookup": {
            "from": "countries",
            "localField": "university.body.country_id",
            "foreignField": "_id",
            "as": "country",
        }
    },
    {"$unwind": {"path": "$country", "preserveNullAndEmptyArrays": True}},
]


=======================this is corrected=====full pipeline===========================

[
    {"$unwind": {"path": "$school_id", "preserveNullAndEmptyArrays": True}},
    {"$unwind": {"path": "$address", "preserveNullAndEmptyArrays": True}},
    {
        "$lookup": {
            "from": "schools",
            "localField": "school_id",
            "foreignField": "_id",
            "as": "school",
        }
    },
    {"$unwind": {"path": "$school", "preserveNullAndEmptyArrays": True}},
    {
        "$lookup": {
            "from": "universities",
            "localField": "school.university_id",
            "foreignField": "_id",
            "as": "university",
        }
    },
    {"$unwind": {"path": "$university", "preserveNullAndEmptyArrays": True}},
    {
        "$unwind": {
            "path": "$university.body",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$lookup": {
            "from": "countries",
            "localField": "university.body.country_id",
            "foreignField": "_id",
            "as": "country",
        }
    },
    {"$unwind": {"path": "$country", "preserveNullAndEmptyArrays": True}},
    {
        "$project": {
            "body_country_name": "$country.country_name",
            "body_name": "$university.body.body_name",
            "name": 1,
            "pincode": "$address.pincode",
            "school_name": "$school.name",
            "state": "$address.state",
            "university_name": "$university.un_name",
            "village": "$address.village",
        }
    },
    {
        "$group": {
            "_id": "$name",
            "address": {
                "$addToSet": {
                    "pincode": "$pincode",
                    "state": "$state",
                    "village": "$village",
                }
            },
            "name": {"$first": "$name"},
            "school": {
                "$addToSet": {
                    "body_country_name": "$body_country_name",
                    "body_name": "$body_name",
                    "school_name": "$school_name",
                    "university_name": "$university_name",
                }
            },
        }
    },
]


"""
# ==================pipeline not working========================
[
    {
        "$lookup": {
            "as": "school_id",
            "foreignField": "_id",
            "from": "schools",
            "localField": "school_id",
        }
    },
    {"$unwind": {"path": "$school_id", "preserveNullAndEmptyArrays": True}},
    {
        "$lookup": {
            "as": "school_id.university_id",
            "foreignField": "_id",
            "from": "universities",
            "localField": "school_id.university_id",
        }
    },
    {
        "$unwind": {
            "path": "$school_id.university_id",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {"$unwind": {"path": "$address", "preserveNullAndEmptyArrays": True}},
    {
        "$unwind": {
            "path": "$school_id.university_id.body",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$project": {
            "body_country_name": "$school_id.university_id.body.country_id.country_name",
            "body_name": "$school_id.university_id.body.body_name",
            "continent_name": "$address.country_id.continent_id.continent_name",
            "country_name": "$address.country_id.country_name",
            "name": "$name",
            "school_name": "$school_id.name",
            "university_name": "$school_id.university_id.un_name",
            "village": "$address.village",
        }
    },
]
