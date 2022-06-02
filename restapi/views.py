# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from decimal import Decimal
import pandas as pd
import numpy as np

from django.http import HttpResponse
from django.contrib.auth.models import User

# Create your views here.
from rest_framework.permissions import AllowAny
from rest_framework.decorators import api_view, action, authentication_classes, permission_classes
from rest_framework.viewsets import ModelViewSet
from rest_framework.response import Response
from rest_framework import status

from restapi.serializers import UserSerializer, GroupSerializer, CategorySerializer, ExpensesSerializer
from restapi.custom_exception import UnauthorizedUserException

from utils import normalize, sort_by_time_stamp, response_format, transform, aggregate, multi_threaded_reader
from typing import Dict, List


def index(_request: Request) -> HttpResponse:
    """ View for the index page"""
    return HttpResponse("Hello, world. You're at Rest.")


@api_view(['POST'])
def logout(request: Request) -> Response:
    """ Logs out the user"""
    request.user.auth_token.delete()
    return Response(status=status.HTTP_204_NO_CONTENT)


@api_view(['GET'])
def balance(request) -> Response:
    """ Fetches the balance for the user"""
    user: User = request.user
    expenses = Expenses.objects.filter(users__in=user.expenses.all())
    final_balance: dict = {}
    for expense in expenses:
        expense_balances: List = normalize(expense)
        for eb in expense_balances:
            from_user: int = eb['from_user']
            to_user: int = eb['to_user']
            if from_user == user.id:
                final_balance[to_user] = final_balance.get(to_user, 0) - eb['amount']
            if to_user == user.id:
                final_balance[from_user] = final_balance.get(from_user, 0) + eb['amount']
    final_balance = {k: v for k, v in final_balance.items() if v != 0}

    response = [{"user": k, "amount": int(v)} for k, v in final_balance.items()]
    return Response(response, status=status.HTTP_200_OK)




class UserViewSet(ModelViewSet):
    queryset = User.objects.all()
    serializer_class = UserSerializer
    permission_classes = (AllowAny,)


class CategoryViewSet(ModelViewSet):
    queryset = Category.objects.all()
    serializer_class = CategorySerializer
    http_method_names = ['get', 'post']


class GroupViewSet(ModelViewSet):
    queryset = Groups.objects.all()
    serializer_class = GroupSerializer

    def get_queryset(self):
        user: User = self.request.user
        groups = user.members.all()
        if self.request.query_params.get('q', None) is not None:
            groups = groups.filter(name__icontains=self.request.query_params.get('q', None))
        return groups

    def create(self, request, *args, **kwargs):
        """ Creates a group"""
        user: User = self.request.user
        data: Dict = self.request.data
        group = Groups(**data)
        group.save()
        group.members.add(user)
        serializer = self.get_serializer(group)
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    @action(methods=['put'], detail=True)
    def members(self, request, pk=None):
        """ Add/remove users from a group"""
        group = Groups.objects.get(id=pk)
        if group not in self.get_queryset():
            raise UnauthorizedUserException()
        body: Dict = request.data
        if body.get('add', None) is not None and body['add'].get('user_ids', None) is not None:
            added_ids = body['add']['user_ids']
            for user_id in added_ids:
                group.members.add(user_id)
        if body.get('remove', None) is not None and body['remove'].get('user_ids', None) is not None:
            removed_ids = body['remove']['user_ids']
            for user_id in removed_ids:
                group.members.remove(user_id)
        group.save()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(methods=['get'], detail=True)
    def expenses(self, _request, pk=None):
        """ Returns the expenses for a group"""
        group = Groups.objects.get(id=pk)
        if group not in self.get_queryset():
            raise UnauthorizedUserException()
        expenses = group.expenses_set
        serializer = ExpensesSerializer(expenses, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @action(methods=['get'], detail=True)
    def balances(self, _request, pk=None) -> Response:
        """ Returns the balance of a group"""
        group = Groups.objects.get(id=pk)
        if group not in self.get_queryset():
            raise UnauthorizedUserException()
        expenses = Expenses.objects.filter(group=group)
        dues: Dict = {}
        for expense in expenses:
            user_balances = UserExpense.objects.filter(expense=expense)
            for user_balance in user_balances:
                dues[user_balance.user] = dues.get(user_balance.user, 0) + user_balance.amount_lent \
                                          - user_balance.amount_owed
        dues: List[Tuple] = [(k, v) for k, v in sorted(dues.items(), key=lambda item: item[1])]
        start: int = 0
        end: int = len(dues) - 1
        balances: List = []
        while start < end:
            amount = min(abs(dues[start][1]), abs(dues[end][1]))
            amount = Decimal(amount).quantize(Decimal(10)**-2)
            user_balance = {"from_user": dues[start][0].id, "to_user": dues[end][0].id, "amount": str(amount)}
            balances.append(user_balance)
            dues[start] = (dues[start][0], dues[start][1] + amount)
            dues[end] = (dues[end][0], dues[end][1] - amount)
            if dues[start][1] == 0:
                start += 1
            else:
                end -= 1

        return Response(balances, status=status.HTTP_200_OK)


class ExpensesViewSet(ModelViewSet):
    queryset = Expenses.objects.all()
    serializer_class = ExpensesSerializer

    def get_queryset(self):
        user = self.request.user
        if self.request.query_params.get('q', None) is not None:
            expenses = Expenses.objects.filter(users__in=user.expenses.all())\
                .filter(description__icontains=self.request.query_params.get('q', None))
        else:
            expenses = Expenses.objects.filter(users__in=user.expenses.all())
        return expenses

@api_view(['post'])
@authentication_classes([])
@permission_classes([])
def log_processor(request) -> Response:
    """ Processes the logs of the request"""
    data = request.data
    num_threads: int = data['parallelFileProcessingCount']
    log_files = data['logFiles']
    if num_threads <= 0 or num_threads > 30:
        return Response({"status": "failure", "reason": "Parallel Processing Count out of expected bounds"},
                        status=status.HTTP_400_BAD_REQUEST)
    if len(log_files) == 0:
        return Response({"status": "failure", "reason": "No log files provided in request"},
                        status=status.HTTP_400_BAD_REQUEST)
    logs: List[str] = multi_threaded_reader(urls=data['logFiles'], num_threads=data['parallelFileProcessingCount'])
    sorted_logs: List[List[str]] = sort_by_time_stamp(logs)
    cleaned: List[List[str]] = transform(sorted_logs)
    data: Dict[str, Dict[str, int]] = aggregate(cleaned)
    response: List[Dict] = response_format(data)
    return Response({"response":response}, status=status.HTTP_200_OK)

