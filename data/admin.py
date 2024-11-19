from django.contrib import admin

from data.models import (
    Pharmacy,
    Supplier,
    Product,
    Order,
    Purchase,
    Sales
)


@admin.register(Pharmacy)
class PharmacyAdmin(admin.ModelAdmin):
    # Variable or functions to show as columns
    list_display = ("id", )


@admin.register(Supplier)
class SupplierAdmin(admin.ModelAdmin):
    # Variable or functions to show as columns
    list_display = ("name", "code_supplier")


@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    # Variable or functions to show as columns
    list_display = ("name", "code_13_ref", "price_with_tax")
    search_fields = ["code_13_ref", 'name']


@admin.register(Order)
class OrderAdmin(admin.ModelAdmin):
    # Variable or functions to show as columns
    list_display = ('id', "step",)

    search_fields = ["id", 'username', 'pharmacy']
    filter_fields = ['supplier']


@admin.register(Purchase)
class PurchaseAdmin(admin.ModelAdmin):
    list_display = ("product__name", "quantity_received", "quantity_expected")

    search_fields = ["product__name"]


@admin.register(Sales)
class SalesAdmin(admin.ModelAdmin):
    list_display = ("product__name", "quantity", "time")

    search_fields = ["product__name"]
