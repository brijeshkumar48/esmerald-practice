import logging
from starlette.background import BackgroundTask, BackgroundTasks
from typing import Any, Optional, Union
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
