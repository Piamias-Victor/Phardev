from django.contrib import admin

from data.models import (
    Pharmacy,
    Supplier,
    GlobalProduct,
    InternalProduct,
    InventorySnapshot,
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


@admin.register(GlobalProduct)
class GlobalProductAdmin(admin.ModelAdmin):
    # Variable or functions to show as columns
    list_display = ("code_13_ref", "name", "brand_lab", "lab_distributor", "universe", "category", "sub_category")
    search_fields = ["brand_lab", "code_13_ref", 'name']
    readonly_fields = ('created_at', 'updated_at', )
    list_filter = ["universe"]


@admin.register(InternalProduct)
class InternalProductAdmin(admin.ModelAdmin):
    # Variable or functions to show as columns
    list_display = ("internal_id", "pharmacy", "name", "code_13_ref", "TVA")
    search_fields = ["code_13_ref__code_13_ref", 'name',  'internal_id']
    readonly_fields = ('created_at', 'updated_at', )
    list_filter = ["pharmacy__name",]
    list_per_page = 25
    raw_id_fields = ('code_13_ref',)


@admin.register(InventorySnapshot)
class InventorySnapshotAdmin(admin.ModelAdmin):
    # Variable or functions to show as columns
    list_display = ("product", "date", "stock", "price_with_tax", "weighted_average_price")
    search_fields = ["product__name",]
    ordering = ('product__name', '-date')
    list_filter = ["product__pharmacy__name",]

    readonly_fields = ('created_at', 'updated_at', )
    raw_id_fields = ('product',)
    list_per_page = 25


@admin.register(Order)
class OrderAdmin(admin.ModelAdmin):
    # Variable or functions to show as columns
    list_display = ('internal_id', "pharmacy", "step", "sent_date", "delivery_date")

    search_fields = ["internal_id",]
    list_filter = ["pharmacy__name", 'step', 'supplier', ]
    ordering = ('-delivery_date',)
    readonly_fields = ('created_at', 'updated_at', )
    list_per_page = 25


@admin.register(ProductOrder)
class ProductOrderAdmin(admin.ModelAdmin):
    list_display = ('order__internal_id', "order__pharmacy", 'product', 'qte', 'qte_r', 'qte_a', 'qte_ug', 'qte_ec', 'qte_ar')

    search_fields = ['order__internal_id', 'pharmacy']
    list_filter = ["order__pharmacy__name",]

    readonly_fields = ('created_at', 'updated_at', )
    list_per_page = 25
    raw_id_fields = ('product',)


@admin.register(Sales)
class SalesAdmin(admin.ModelAdmin):
    list_display = ("product", "quantity", "date")

    search_fields = ["product"]
    readonly_fields = ('created_at', 'updated_at', )
    list_per_page = 25
    raw_id_fields = ('product',)
