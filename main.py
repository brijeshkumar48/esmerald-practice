# from esmerald import Esmerald
# from app.views import upload_file

# app = Esmerald(routes=[upload_file])

import uvicorn
from esmerald import Esmerald
from app.urls import urlpatterns

app = Esmerald(routes=urlpatterns)

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
