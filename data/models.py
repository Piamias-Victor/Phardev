from django.db import models


class Pharmacy(models.Model):
    id = models.AutoField(primary_key=True)

    def __str__(self):
        return f"Pharmacy {self.id}"


class Supplier(models.Model):
    code_supplier = models.CharField(max_length=255)
    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name


class Product(models.Model):
    id = models.PositiveBigIntegerField(primary_key=True)
    code_13_ref = models.CharField(max_length=13, unique=True)
    name = models.CharField(max_length=255)
    price_with_tax = models.DecimalField(max_digits=10, decimal_places=2)

    def __str__(self):
        return self.name


class Order(models.Model):
    id = models.PositiveBigIntegerField(primary_key=True)
    step = models.PositiveSmallIntegerField()
    sent = models.DateTimeField()
    delivery_date = models.DateField()
    supplier = models.ForeignKey(Supplier, on_delete=models.CASCADE)
    pharmacy = models.ForeignKey(Pharmacy, on_delete=models.CASCADE)

    def __str__(self):
        return f"Order {self.id}"


class Purchase(models.Model):
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    order = models.ForeignKey(Order, on_delete=models.CASCADE)
    quantity_received = models.PositiveSmallIntegerField()
    quantity_expected = models.PositiveSmallIntegerField()
    bulk_quantity = models.PositiveSmallIntegerField()
    in_progress_quantity = models.PositiveSmallIntegerField()
    quantity_to_receive = models.PositiveSmallIntegerField()

    def __str__(self):
        return f"Purchase for {self.product.name} in Order {self.order.id}"


class Sales(models.Model):
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    quantity = models.IntegerField()
    time = models.DateTimeField()

    def __str__(self):
        return f"Sales of {self.quantity} for {self.quantity.name}"
