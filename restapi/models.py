# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import constants

from django.db import models

# Create your models here.
from django.contrib.auth.models import User


class Category(models.Model):
    name = models.CharField(max_length=constants.CATEGORY_NAME_MAX_LENGTH, null=False)


class Groups(models.Model):
    name = models.CharField(max_length=constants.GROUP_NAME_MAX_LENGTH, null=False)
    members = models.ManyToManyField(User, related_name='members', blank=True)


class Expenses(models.Model):
    description = models.CharField(max_length=constants.EXPENSES_DESCRIPTION_MAX_LENGTH)
    total_amount = models.DecimalField(
        max_digits=constants.EXPENSES_AMOUNT_MAX_DIGITS, 
        decimal_places=constants.EXPENSES_AMOUNT_DECIMAL_PLACES
    )
    group = models.ForeignKey(Groups, null=True, on_delete=models.CASCADE)
    category = models.ForeignKey(Category, default=1, on_delete=models.CASCADE)


class UserExpense(models.Model):
    expense = models.ForeignKey(Expenses, default=1, on_delete=models.CASCADE, related_name="users")
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="expenses")
    amount_owed = models.DecimalField(
        max_digits=constants.USER_EXPENSE_AMOUNT_OWED_MAX_DIGITS, 
        decimal_places=constants.USER_EXPENSE_AMOUNT_OWED_DECIMAL_PLACES
    )
    amount_lent = models.DecimalField(
        max_digits=constants.USER_EXPENSE_AMOUNT_LENT_MAX_DIGITS, 
        decimal_places=constants.USER_EXPENSE_AMOUNT_LENT_DECIMAL_PLACES
    )

    def __str__(self):
        return f"user: {self.user}, amount_owed: {self.amount_owed} amount_lent: {self.amount_lent}"
