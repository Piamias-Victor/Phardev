import uuid
from django.core.validators import MinValueValidator

from django.db import models
from django.db.models import UniqueConstraint


class Pharmacy(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    id_nat = models.CharField(max_length=255, null=True, blank=True,)
    name = models.CharField(max_length=255, null=True, blank=True,)

    ca = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True, verbose_name="Chiffre d'affaires")
    area = models.CharField(max_length=255, null=True, blank=True, verbose_name="Zone géographique")
    employees_count = models.PositiveSmallIntegerField(null=True, blank=True, verbose_name="Nombre d'employés")
    address = models.TextField(null=True, blank=True, verbose_name="Adresse")

    def __str__(self):
        return f"Pharmacy {self.id_nat}"


class Supplier(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    pharmacy = models.ForeignKey(Pharmacy, on_delete=models.CASCADE)
    code_supplier = models.CharField(max_length=255, db_index=True)
    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name

    class Meta:
        constraints = [
            UniqueConstraint(fields=['pharmacy', 'code_supplier'], name='unique_supplier_constraint')
        ]


class GlobalProduct(models.Model):
    code_13_ref = models.CharField(max_length=13, primary_key=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    name = models.TextField(verbose_name="Produit")
    year = models.PositiveSmallIntegerField(verbose_name="Date", null=True, blank=True)

    universe = models.CharField(max_length=255, verbose_name="Univers")
    category = models.CharField(max_length=255, null=True, blank=True, verbose_name="Catégorie")
    sub_category = models.CharField(max_length=255, null=True, blank=True, verbose_name="Sous catégorie")
    brand_lab = models.CharField(max_length=255, blank=True, null=True, verbose_name="Marque - Labo")
    lab_distributor = models.CharField(max_length=255, blank=True, null=True, verbose_name="Laboratoire - Distributeur")
    range_name = models.CharField(max_length=255, blank=True, null=True, verbose_name="Gamme")
    specificity = models.CharField(max_length=255, blank=True, null=True, verbose_name="Spécificité")
    family = models.CharField(max_length=255, blank=True, null=True, verbose_name="Famille")
    sub_family = models.CharField(max_length=255, blank=True, null=True, verbose_name="Sous famille")
    tva_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True, verbose_name="% TVA")
    free_access = models.BooleanField(verbose_name="Libre accès", blank=True, null=True)

    def __str__(self):
        return self.name


class InternalProduct(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    internal_id = models.PositiveIntegerField()
    code_13_ref = models.ForeignKey(GlobalProduct, null=True, blank=True, on_delete=models.CASCADE)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    pharmacy = models.ForeignKey(Pharmacy, on_delete=models.CASCADE)
    name = models.CharField(max_length=255)
    TVA = models.DecimalField(
        max_digits=4,
        decimal_places=2,
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        verbose_name="TVA"
    )

    def __str__(self):
        return f"{self.internal_id} {self.name}"

    class Meta:
        constraints = [
            UniqueConstraint(fields=['pharmacy', 'internal_id'], name='unique_product_constraint')
        ]


class InventorySnapshot(models.Model):
    #TODO Partitioning
    id = models.BigAutoField(primary_key=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    product = models.ForeignKey(InternalProduct, on_delete=models.CASCADE, related_name="snapshot_history")
    date = models.DateField()

    stock = models.SmallIntegerField(default=0, verbose_name="Stock")
    price_with_tax = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    weighted_average_price = models.DecimalField(max_digits=10, decimal_places=2, default=0, null=True, blank=True)

    class Meta:
        constraints = [
            UniqueConstraint(fields=['product', 'date'], name='unique_snapshot_constraint')
        ]
        indexes = [
            models.Index(fields=['product', 'date']),
            models.Index(fields=['date']),
            models.Index(fields=['product']),
        ]


class Order(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    internal_id = models.PositiveBigIntegerField()
    supplier = models.ForeignKey(Supplier, on_delete=models.CASCADE, related_name="orders")
    pharmacy = models.ForeignKey(Pharmacy, on_delete=models.CASCADE, related_name="orders")

    step = models.PositiveSmallIntegerField()
    sent_date = models.DateTimeField(blank=True, null=True)
    delivery_date = models.DateField(blank=True, null=True)

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
    product = models.ForeignKey(
        InternalProduct,
        on_delete=models.CASCADE,
        related_name="product_orders",
        verbose_name="Product"
    )
    order = models.ForeignKey(
        Order,
        on_delete=models.CASCADE,
        related_name="product_orders",
        verbose_name="Order"
    )

    qte = models.PositiveSmallIntegerField(verbose_name="Quantité")
    qte_r = models.PositiveSmallIntegerField(verbose_name="Quantité Réceptionnée")
    qte_a = models.SmallIntegerField(verbose_name="Quantité Attendue")
    qte_ug = models.PositiveSmallIntegerField(verbose_name="Quantité Unité Gratuite")
    qte_ec = models.SmallIntegerField(verbose_name="Quantité en Écart")
    qte_ar = models.SmallIntegerField(verbose_name="Quantité à Réceptionner")

    class Meta:
        constraints = [
            UniqueConstraint(fields=['order', 'product'], name='unique_productorder_constraint')
        ]
        verbose_name = "Product Order"
        verbose_name_plural = "Product Orders"


class Sales(models.Model):
    id = models.BigAutoField(primary_key=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    product = models.ForeignKey(
        InventorySnapshot,
        on_delete=models.CASCADE,
        related_name="sales_history",
        verbose_name="Inventory Snapshot"
    )

    quantity = models.SmallIntegerField()
    date = models.DateField(db_index=True, verbose_name="Date")

    def __str__(self):
        return f"{self.quantity} Sales for {self.product} on {self.date}"

    class Meta:
        constraints = [
            UniqueConstraint(fields=['date', 'product'], name='unique_dateproductsale_constraint')
        ]
