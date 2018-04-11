from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^searchbyimage/', views.searchByImage, name='searchByImage'),
]
