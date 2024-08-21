from typing import Any
from django.core.management.base import BaseCommand
from analysis.logic import UploadCleanDataSet

class Commands(BaseCommand):

    def handle(self, *args: Any, **options: Any) -> str | None:
        UploadCleanDataSet().processDataSetToDatabase(path="")