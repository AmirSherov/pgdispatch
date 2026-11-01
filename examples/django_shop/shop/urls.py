from django.urls import path

from orders import views

urlpatterns = [
    path("orders", views.create_order),
    path("orders/<int:order_id>/ship", views.ship_order),
]
