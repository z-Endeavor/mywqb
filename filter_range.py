from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from math import inf
from math import isinf as __isinf
from typing import Any, Self

__all__ = ['FilterRange']


def _isinf(
    x: Any,
) -> bool:
    return isinstance(x, float) and __isinf(x)


def _parse_ifd(
    val: str,
) -> int | float | datetime:
    val = val.strip()
    try:
        val = datetime.fromisoformat(val)
    except ValueError as e:
        if val[0] not in '-+':
            val = '+' + val
        val_abs = val[1:].lstrip()
        val_abs = float(val_abs) if 'inf' == val_abs or '.' in val_abs else int(val_abs)
        val = -val_abs if '-' == val[0] else val_abs
    return val


@dataclass(frozen=True, slots=True)
class FilterRange:

    lo: int | float | datetime = field(default=-inf, kw_only=False)
    hi: int | float | datetime = field(default=inf, kw_only=False)
    lo_eq: bool = field(default=False, kw_only=False)
    hi_eq: bool = field(default=False, kw_only=False)

    def __post_init__(
        self,
    ) -> None:
        if not self.lo <= self.hi:
            raise ValueError(f"not <{self.lo=}> <= <{self.hi=}>")
        if self.lo == self.hi and not (self.lo_eq and self.hi_eq):
            raise ValueError(
                f"<{self.lo=}> == <{self.hi=}> and not (<{self.lo_eq=}> and <{self.hi_eq=}>)"
            )
        if _isinf(self.lo) and self.lo_eq:
            raise ValueError(f"isinf(<{self.lo=}>) and <{self.lo_eq=}>")
        if _isinf(self.hi) and self.hi_eq:
            raise ValueError(f"isinf(<{self.hi=}>) and <{self.hi_eq=}>")

    @classmethod
    def from_str(
        cls,
        target: str,
    ) -> Self:
        try:
            pair = target.split(sep=',')
            if 2 != len(pair):
                raise ValueError(f"'{target}' is invalid.")
            lft = pair[0].strip()
            rit = pair[1].strip()
            if '[' == lft[0]:
                lo_eq = True
            elif '(' == lft[0]:
                lo_eq = False
            else:
                raise ValueError(f"'{lft[0]}' in '{target}' is invalid.")
            if ']' == rit[-1]:
                hi_eq = True
            elif ')' == rit[-1]:
                hi_eq = False
            else:
                raise ValueError(f"'{rit[-1]}' in '{target}' is invalid.")
            lo = _parse_ifd(lft[1:])
            hi = _parse_ifd(rit[:-1])
        except IndexError as e:
            raise ValueError(f"'{target}' is invalid.")
        return cls(lo, hi, lo_eq, hi_eq)

    @classmethod
    def from_conditions(
        cls,
        target: Iterable[str],
    ) -> Self:
        self_lo = -inf
        self_hi = inf
        self_lo_eq = False
        self_hi_eq = False
        for condition in target:
            try:
                condition = condition.strip()
                op = condition[:1]
                if '>' == op:
                    lo_eq = '=' == condition[1]
                    lo = _parse_ifd(condition[2 if lo_eq else 1 :])
                    if self_lo < lo:
                        self_lo = lo
                        self_lo_eq = lo_eq
                    elif self_lo == lo and self_lo_eq and not lo_eq:  # [(...
                        self_lo_eq = lo_eq
                elif '<' == op:
                    hi_eq = '=' == condition[1]
                    hi = _parse_ifd(condition[2 if hi_eq else 1 :])
                    if hi < self_hi:
                        self_hi = hi
                        self_hi_eq = hi_eq
                    elif hi == self_hi and self_hi_eq and not hi_eq:  # ...)]
                        self_hi_eq = hi_eq
                elif '=' == op:
                    lo_eq = hi_eq = True
                    lo = hi = _parse_ifd(condition[1:])
                    if self_lo < lo:
                        self_lo = lo
                        self_lo_eq = lo_eq
                    elif self_lo == lo and self_lo_eq and not lo_eq:  # [(...
                        self_lo_eq = lo_eq
                    if hi < self_hi:
                        self_hi = hi
                        self_hi_eq = hi_eq
                    elif hi == self_hi and not hi_eq and self_hi_eq:  # ...)]
                        self_hi_eq = hi_eq
                else:
                    raise ValueError(f"'{op}' in '{condition}' is invalid.")
            except IndexError as e:
                raise ValueError(f"'{condition}' is invalid.")
        return cls(self_lo, self_hi, self_lo_eq, self_hi_eq)

    @classmethod
    def parse(
        cls,
        target: str | Iterable[str],
    ) -> Self:
        if isinstance(target, str):
            return cls.from_str(target)
        if isinstance(target, Iterable):
            return cls.from_conditions(target)

    def to_str(
        self,
    ) -> str:
        return (
            ('[' if self.lo_eq else '(')
            + (self.lo.isoformat() if isinstance(self.lo, datetime) else str(self.lo))
            + ', '
            + (self.hi.isoformat() if isinstance(self.hi, datetime) else str(self.hi))
            + (']' if self.hi_eq else ')')
        )

    def to_conditions(
        self,
        *,
        try_eq: bool = True,
        inf_as: str | None = None,
    ) -> list[str]:
        if try_eq and self.lo == self.hi:
            return ['=' + str(self.lo)]
        conditions = []
        if not (_isinf(self.lo) and inf_as is None):
            conditions.append(
                ('>=' if self.lo_eq else '>')
                + (
                    self.lo.isoformat()
                    if isinstance(self.lo, datetime)
                    else '-' + inf_as if _isinf(self.lo) else str(self.lo)
                )
            )
        if not (_isinf(self.hi) and inf_as is None):
            conditions.append(
                ('<=' if self.hi_eq else '<')
                + (
                    self.hi.isoformat()
                    if isinstance(self.hi, datetime)
                    else inf_as if _isinf(self.hi) else str(self.hi)
                )
            )
        return conditions

    def to_params(
        self,
        whose: str,
        **kwargs,
    ) -> str:
        return '&'.join(
            (whose + condition for condition in self.to_conditions(**kwargs))
        )