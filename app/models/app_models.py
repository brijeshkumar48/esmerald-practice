from mongoz import String, Document, fields
from typing import Annotated, Any, ClassVar, List, Optional, Type, Union
import mongoz
from pydantic import Field
from app.baseModel import BaseDocument
from app.models.model_resolver import get_zone_model
from db.registry import registry
from bson import ObjectId
from mongoz import Document, EmbeddedDocument
from mongoz.core.db.fields.core import FieldFactory, CLASS_DEFAULTS
from mongoz.core.db.fields.base import BaseField


class ForeignKey(FieldFactory, ObjectId):
    _type = ObjectId

    def __new__(
        cls,
        model: Union[Type[Document], Type[EmbeddedDocument]],
        null: bool = False,
        **kwargs: Any,
    ) -> BaseField:
        kwargs = {
            **kwargs,
            **{
                key: value
                for key, value in locals().items()
                if key not in CLASS_DEFAULTS
            },
            # "null": null,
            # "json_schema_extra": {
            #     "Meta": {
            #         "ref": model.__name__,
            #         "ref_model": model,
            #     }
            # },
        }
        return super().__new__(cls, **kwargs)


FileUploadResultTypeChoices = (
    ("SN", "SystemNudge"),
    ("BN", "BusinessNudge"),
    ("B", "Badge"),
)


class UploadedMediaFile(BaseDocument):
    """
    Model representing uploaded media files
    """

    name: str = String(max_length=48)
    path: str = String(max_length=124)
    type: str = String(max_length=2, choices=FileUploadResultTypeChoices)

    class Meta:
        registry = registry
        database = "test_database"

    @staticmethod
    def get_dbtype():
        return "uploaded_media_file"


ContinentChoices = (
    ("AF", "Africa"),
    ("AN", "Antarctica"),
    ("AS", "Asia"),
    ("EU", "Europe"),
    ("NA", "North America"),
    ("OC", "Oceania"),
    ("SA", "South America"),
)


class Continent(BaseDocument):
    continent_name: str = String(max_length=20, choices=ContinentChoices)
    zone_id: ObjectId = ForeignKey(lambda: Zone)

    class Meta:
        registry = registry
        database = "test_database"
        collection = "continents"


class Zone(BaseDocument):
    zone_name: str = mongoz.String()

    class Meta:
        registry = registry
        database = "test_database"
        collection = "zone"


class Country(BaseDocument):
    country_name: str = mongoz.String()
    continent_id: ObjectId = ForeignKey(Continent)

    class Meta:
        registry = registry
        database = "test_database"
        collection = "countries"


class Bodies(EmbeddedDocument):
    body_name: str = mongoz.String()
    country_id: ObjectId = ForeignKey(Country)

    class Meta:
        registry = registry
        database = "test_database"
        collection = "bodies"


class University(BaseDocument):
    un_name: str = mongoz.String()
    body: List[Bodies] = mongoz.Array(Bodies, default=[])

    class Meta:
        registry = registry
        database = "test_database"
        collection = "universities"


SchoolBoardChoices = (
    ("ST", "State Board"),
    ("CB", "CBSE"),
    ("IC", "ICSC"),
)


class School(BaseDocument):
    name: str = mongoz.String()
    board: str = mongoz.String(choices=SchoolBoardChoices)
    university_id: ObjectId = ForeignKey(University)

    class Meta:
        registry = registry
        database = "test_database"
        collection = "schools"


class Address(EmbeddedDocument):
    village: str = mongoz.String()
    state: str = mongoz.String()
    pincode: str = mongoz.Integer()
    country_id: ObjectId = ForeignKey(Country)

    class Meta:
        registry = registry
        database = "test_database"
        collection = "address"

    @staticmethod
    def get_dbtype():
        return "address"


class Batch(BaseDocument):
    batch_no = mongoz.String()

    class Meta:
        registry = registry
        database = "test_database"
        collection = "batches"

    @staticmethod
    def get_dbtype():
        return "batches"


class Generation(BaseDocument):
    generation: str = mongoz.String()

    class Meta:
        registry = registry
        database = "test_database"
        collection = "generations"

    @classmethod
    def get_dbtype(cls) -> str:
        return "generations"


class Student(BaseDocument):
    name: str = mongoz.String()
    std: str = mongoz.String()
    roll_no: int = mongoz.Integer()
    obtained_pct: float = mongoz.Decimal(decimal_places=5, max_digits=10)
    is_pass: bool = (mongoz.Boolean(),)
    mobile_number: str = mongoz.String()
    school_id: ObjectId = ForeignKey(School)
    batch_id: ObjectId = ForeignKey(Batch)
    # address: Address = mongoz.EmbeddedDocument()
    address: List[Address] = mongoz.Array(Address, default=[])
    stu_generation_id: ObjectId = ForeignKey(Generation)

    class Meta:
        registry = registry
        database = "test_database"
        collection = "students"

    @classmethod
    def get_dbtype(cls) -> str:
        return "students"


class Section(BaseDocument):
    section: str = mongoz.String()
    std: str = mongoz.String()
    mobile_number: str = mongoz.String()
    school_id: ObjectId = ForeignKey(School)
    batch_id: ObjectId = ForeignKey(Batch)
    generation_id: ObjectId = ForeignKey(Generation)

    class Meta:
        registry = registry
        database = "test_database"
        collection = "sections"
