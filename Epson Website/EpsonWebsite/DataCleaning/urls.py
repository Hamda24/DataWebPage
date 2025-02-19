from django.urls import path
from .views import index, upload_files

urlpatterns = [
    path('', index, name='home'),
    path('upload/', upload_files, name='upload_files'),
]