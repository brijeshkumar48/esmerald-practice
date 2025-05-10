import logging
from starlette.background import BackgroundTask, BackgroundTasks
from typing import Any, List, Optional, Union
from esmerald.conf import settings
import json
from esmerald import Response, Request

logger = logging.getLogger(__name__)


def generate_response(
    request: Request,
    data: dict | list,
    message: str,
    info: list = None,
    background: Optional[Union[BackgroundTask, BackgroundTasks]] = None,
    extra_headers: dict = None,
) -> Response:
    try:
        response_headers = {
            "accept-language": request.headers.get("accept-language", "en")
        }

        if not info:
            info = []

        if isinstance(data, dict):
            data.update({"info": info})

        if isinstance(extra_headers, dict) and extra_headers:
            response_headers.update(extra_headers)

        response_dict = {"message": message, "data": data}

        response = Response(
            response_dict, headers=response_headers, background=background
        )
        return response

    except Exception as e:
        logger.error(e)
        return Response(
            content={"message": f"Internal Server Error:{e}"}, status_code=500
        )


from functools import wraps
from esmerald import Response
import re


def validate_uploaded_files_path():
    """
    Decorator to validate that the file_path starts with 'uploaded-files/'
    and contains only allowed characters.
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Extract file_path from kwargs (depends on your handler signature)
            file_path = kwargs.get("file_path")

            # Validate the path
            if not re.fullmatch(
                r"^uploaded-files/[a-zA-Z0-9_\-\/.]*$", file_path
            ):
                return Response(
                    {
                        "detail": "Invalid path. Must follow 'uploaded-files/{filename}'."
                    },
                    status_code=400,
                )

            # Proceed if validation passes
            return await func(*args, **kwargs)

        return wrapper

    return decorator
