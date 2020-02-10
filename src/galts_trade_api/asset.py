import datetime
from typing import Optional


class Asset:
    def __init__(
        self,
        id: int,
        tag: str,
        name: str,
        precision: int,
        create_time: datetime.datetime,
        delete_time: Optional[datetime.datetime]
    ):
        self._id: int = int(id)
        self._tag: str = str(tag).strip()
        self._name: str = str(name).strip()
        self._precision: int = int(precision)
        self._create_time: datetime.datetime = create_time
        self._delete_time: Optional[datetime.datetime] = delete_time

    @property
    def id(self):
        return self._id

    @property
    def tag(self):
        return self._tag

    @property
    def name(self):
        return self._name

    @property
    def precision(self):
        return self._precision

    @property
    def create_time(self):
        return self._create_time

    @property
    def delete_time(self):
        return self._delete_time


class Symbol:
    @classmethod
    def form_tag(cls, base_asset_tag: str, quote_asset_tag: str) -> str:
        return f'{base_asset_tag}{quote_asset_tag}'

    def __init__(
        self,
        id: int,
        base_asset_id: int,
        quote_asset_id: int,
        create_time: datetime.datetime,
        delete_time: Optional[datetime.datetime]
    ):
        self._id: int = int(id)
        self._base_asset_id: int = int(base_asset_id)
        self._quote_asset_id: int = int(quote_asset_id)
        self._create_time: datetime.datetime = create_time
        self._delete_time: Optional[datetime.datetime] = delete_time

    @property
    def id(self):
        return self._id

    @property
    def base_asset_id(self):
        return self._base_asset_id

    @property
    def quote_asset_id(self):
        return self._quote_asset_id

    @property
    def create_time(self):
        return self._create_time

    @property
    def delete_time(self):
        return self._delete_time
