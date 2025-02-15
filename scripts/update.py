import os
import sys
import re
import django
import pandas as pd
from tqdm import tqdm
from decimal import Decimal, InvalidOperation
from django.db import transaction

# 1. Configuration Django
# -----------------------------------------------------------------------------
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Phardev.settings')
django.setup()
from datetime import date
from django.db.models import Sum

# 2. Imports Django
# -----------------------------------------------------------------------------
from data.models import Pharmacy, GlobalProduct,Sales, InventorySnapshot, InternalProduct

pharmacies = Pharmacy.objects.filter(name__isnull=False)
for pharma in pharmacies:

    print(Sales.objects.filter(product__product__pharmacy=pharma).delete())
    print(InventorySnapshot.objects.filter(product__pharmacy=pharma).delete())
    print(InternalProduct.objects.filter(pharmacy=pharma).delete())

    print(pharma.delete())
