from django.db import models
import uuid
from django.db.models import UniqueConstraint


class Pharmacy(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    name = models.CharField(max_length=255)

    def __str__(self):
        return f"Pharmacy {self.name}"


class Supplier(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    code_supplier = models.CharField(max_length=255)
    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name

    class Meta:
        constraints = [
            UniqueConstraint(fields=['code_supplier', 'name'], name='unique_supplier_constraint')
        ]


class Product(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    internal_id = models.PositiveBigIntegerField()
    pharmacy = models.ForeignKey(Pharmacy, on_delete=models.CASCADE)

    code_13_ref = models.CharField(max_length=13, null=True, blank=True)
    name = models.CharField(max_length=255)
    stock = models.IntegerField(default=0, verbose_name="Stock")

    TVA = models.DecimalField(max_digits=4, decimal_places=2, null=True, blank=True)
    price_with_tax = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    weighted_average_price = models.DecimalField(max_digits=10, decimal_places=2, default=0, null=True, blank=True)

    def __str__(self):
        return self.name

    class Meta:
        constraints = [
            UniqueConstraint(fields=['pharmacy', 'internal_id', 'code_13_ref'], name='unique_product_constraint')
        ]
        
        
class Order(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    internal_id = models.PositiveBigIntegerField()
    supplier = models.ForeignKey(Supplier, on_delete=models.CASCADE)
    pharmacy = models.ForeignKey(Pharmacy, on_delete=models.CASCADE)

    step = models.PositiveSmallIntegerField()
    sent_date = models.DateTimeField()
    delivery_date = models.DateField()

    def __str__(self):
        return f"Order"

    class Meta:
        constraints = [
            UniqueConstraint(fields=['internal_id', 'pharmacy'], name='unique_order_constraint')
        ]


class ProductOrder(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    order = models.ForeignKey(Order, on_delete=models.CASCADE)

    qte = models.IntegerField(verbose_name="Quantité")
    qte_r = models.IntegerField(verbose_name="Quantité Réceptionnée")
    qte_a = models.IntegerField(verbose_name="Quantité Attendue")
    qte_ug = models.IntegerField(verbose_name="Quantité Unité de Gestion")
    qte_ec = models.IntegerField(verbose_name="Quantité en Écart")
    qte_ar = models.IntegerField(verbose_name="Quantité à Réceptionner")

    class Meta:
        constraints = [
            UniqueConstraint(fields=['order', 'product'], name='unique_productorder_constraint')
        ]


class Sales(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    pharmacy = models.ForeignKey(Pharmacy, on_delete=models.CASCADE)
    product = models.ForeignKey(Product, on_delete=models.CASCADE)

    quantity = models.IntegerField()
    time = models.DateTimeField()

    def __str__(self):
        return f"{self.quantity} Sales"

    class Meta:
        constraints = [
            UniqueConstraint(fields=['time', 'product', 'pharmacy'], name='unique_sale_constraint')
        ]