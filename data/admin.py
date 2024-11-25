from django.contrib import admin

from data.models import (
    Pharmacy,
    Supplier,
    Product,
    Order,
    ProductOrder,
    Sales
)


@admin.register(Pharmacy)
class PharmacyAdmin(admin.ModelAdmin):
    # Variable or functions to show as columns
    list_display = ("id", 'name')
    readonly_fields = ('created_at', 'updated_at', )


@admin.register(Supplier)
class SupplierAdmin(admin.ModelAdmin):
    # Variable or functions to show as columns
    list_display = ("name", "code_supplier")
    readonly_fields = ('created_at', 'updated_at', )


@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    # Variable or functions to show as columns
    list_display = ("internal_id", "pharmacy", "name", "code_13_ref", "stock", "price_with_tax")
    search_fields = ["code_13_ref", 'name']
    readonly_fields = ('created_at', 'updated_at', )
    list_filter = ["pharmacy__name",]


@admin.register(Order)
class OrderAdmin(admin.ModelAdmin):
    # Variable or functions to show as columns
    list_display = ('internal_id', "pharmacy", "step", "sent_date", "delivery_date")

    search_fields = ["id", 'username', 'pharmacy']
    list_filter = ["pharmacy__name", 'step', 'supplier', ]
    ordering = ('-delivery_date',)
    readonly_fields = ('created_at', 'updated_at', )


@admin.register(ProductOrder)
class ProductOrderAdmin(admin.ModelAdmin):
    list_display = ('order__internal_id', "order__pharmacy", 'product', 'qte', 'qte_r', 'qte_a', 'qte_ug', 'qte_ec', 'qte_ar')

    search_fields = ['order__internal_id', 'pharmacy']
    list_filter = ["order__pharmacy__name", 'product']

    readonly_fields = ('created_at', 'updated_at', )


@admin.register(Sales)
class SalesAdmin(admin.ModelAdmin):
    list_display = ("product", 'pharmacy__name', "quantity", "time")

    search_fields = ["product"]
    readonly_fields = ('created_at', 'updated_at', )
    list_filter = ["pharmacy__name"]
