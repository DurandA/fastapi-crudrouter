from typing import Any, Callable, List, Type, Generator, Optional, Union

from fastapi import Depends, HTTPException

from . import CRUDGenerator, NOT_FOUND, _utils
from ._types import DEPENDENCIES, PAGINATION, PYDANTIC_SCHEMA as SCHEMA
import inspect

try:
    from sqlalchemy.orm import Session, immediateload
    from sqlalchemy.ext.declarative import DeclarativeMeta as Model
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.future import select
    from sqlalchemy import __version__ as sqlalchemy_version        
except ImportError:
    Model = None
    Session = None
    IntegrityError = None
    sqlalchemy_installed = False
else:
    sqlalchemy_installed = True
    Session = Callable[..., Generator[Session, Any, None]]

CALLABLE = Callable[..., Model]
CALLABLE_LIST = Callable[..., List[Model]]


class SQLAlchemyCRUDRouter(CRUDGenerator[SCHEMA]):
    def __init__(
        self,
        schema: Type[SCHEMA],
        db_model: Model,
        db: "Session",
        create_schema: Optional[Type[SCHEMA]] = None,
        update_schema: Optional[Type[SCHEMA]] = None,
        prefix: Optional[str] = None,
        tags: Optional[List[str]] = None,
        paginate: Optional[int] = None,
        get_all_route: Union[bool, DEPENDENCIES] = True,
        get_one_route: Union[bool, DEPENDENCIES] = True,
        create_route: Union[bool, DEPENDENCIES] = True,
        update_route: Union[bool, DEPENDENCIES] = True,
        delete_one_route: Union[bool, DEPENDENCIES] = True,
        delete_all_route: Union[bool, DEPENDENCIES] = True,
        use_async: Optional[bool] = None,  # if not set, try autodetect
        **kwargs: Any
    ) -> None:
        assert (
            sqlalchemy_installed
        ), "SQLAlchemy must be installed to use the SQLAlchemyCRUDRouter."

        self.db_model = db_model
        self.db_func = db
        if use_async == None:
            self.use_async = (
                inspect.isasyncgenfunction(db) or inspect.isasyncgen(db)
            ) and sqlalchemy_version >= "1.4"  # autodetect async mode
        else:
            self.use_async = use_async
        self._pk: str = db_model.__table__.primary_key.columns.keys()[0]
        self._pk_type: type = _utils.get_pk_type(schema, self._pk)

        super().__init__(
            schema=schema,
            create_schema=create_schema,
            update_schema=update_schema,
            prefix=prefix or db_model.__tablename__,
            tags=tags,
            paginate=paginate,
            get_all_route=get_all_route,
            get_one_route=get_one_route,
            create_route=create_route,
            update_route=update_route,
            delete_one_route=delete_one_route,
            delete_all_route=delete_all_route,
            **kwargs
        )

    def _get_all(self, *args: Any, **kwargs: Any) -> CALLABLE_LIST:
        async def route(
            db: Session = Depends(self.db_func),
            pagination: PAGINATION = self.pagination,
        ) -> List[Model]:
            skip, limit = pagination.get("skip"), pagination.get("limit")

            res = db.execute(
                select(self.db_model)
                .order_by(getattr(self.db_model, self._pk))
                .limit(limit)
                .offset(skip)
            )
            if inspect.isawaitable(res):
                res = await res
            res = res.all()

            model: Model
            db_models: List[Model] = []
            for row in res:
                (model,) = row
                db_models.append(model)

            return db_models

        return route

    def _get_one(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(
            item_id: self._pk_type, db: Session = Depends(self.db_func)  # type: ignore
        ) -> Model:
            model = db.get(self.db_model, item_id)
            if inspect.isawaitable(model):
                model = await model

            if model:
                return model
            else:
                raise NOT_FOUND from None

        return route

    def _create(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(
            model: self.create_schema,  # type: ignore
            db: Session = Depends(self.db_func),
        ) -> Model:
            try:
                db_model: Model = self.db_model(**model.dict())
                db.add(db_model)
                if inspect.isawaitable(res := db.commit()):
                    await res
                if inspect.isawaitable(res := db.refresh(db_model)):
                    await res
                return db_model
            except IntegrityError:
                if inspect.isawaitable(res := db.rollback()):
                    await res
                raise HTTPException(422, "Key already exists") from None

        return route

    def _update(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(
            item_id: self._pk_type,  # type: ignore
            model: self.update_schema,  # type: ignore
            db: Session = Depends(self.db_func),
        ) -> Model:
            try:
                db_model: Model = await self._get_one()(item_id, db)

                for key, value in model.dict(exclude={self._pk}).items():
                    if hasattr(db_model, key):
                        setattr(db_model, key, value)

                if inspect.isawaitable(res := db.commit()):
                    await res
                if inspect.isawaitable(res := db.refresh(db_model)):
                    await res

                return db_model
            except IntegrityError as e:
                if inspect.isawaitable(res := db.rollback()):
                    await res
                self._raise(e)

        return route

    def _delete_all(self, *args: Any, **kwargs: Any) -> CALLABLE_LIST:
        async def route(db: Session = Depends(self.db_func)) -> List[Model]:
            if inspect.isawaitable(res := db.execute("delete from " + self.db_model.__tablename__)):
                await res
            if inspect.isawaitable(res := db.commit()):
                await res
            return await self._get_all()(db=db, pagination={"skip": 0, "limit": None})

        return route

    def _delete_one(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(
            item_id: self._pk_type, db: Session = Depends(self.db_func)  # type: ignore
        ) -> Model:
            db_model: Model = await self._get_one()(item_id, db)
            if inspect.isawaitable(res := db.delete(db_model)):
                await res
            if inspect.isawaitable(res := db.commit()):
                await res

            return db_model

        return route
