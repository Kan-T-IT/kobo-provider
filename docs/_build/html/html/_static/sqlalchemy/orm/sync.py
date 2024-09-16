# orm/sync.py
# Copyright (C) 2005-2024 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

"""private module containing functions used for copying data
between instances based on join conditions.

"""

from . import attributes
from . import exc
from . import util as orm_util
from .. import util


def populate(
    source,
    source_mapper,
    dest,
    dest_mapper,
    synchronize_pairs,
    uowcommit,
    flag_cascaded_pks,
):
    source_dict = source.dict
    dest_dict = dest.dict

    for l, r in synchronize_pairs:
        try:
            # inline of source_mapper._get_state_attr_by_column
            prop = source_mapper._columntoproperty[l]
            value = source.manager[prop.key].impl.get(
                source, source_dict, attributes.PASSIVE_OFF
            )
        except exc.UnmappedColumnError as err:
            _raise_col_to_prop(False, source_mapper, l, dest_mapper, r, err)

        try:
            # inline of dest_mapper._set_state_attr_by_column
            prop = dest_mapper._columntoproperty[r]
            dest.manager[prop.key].impl.set(dest, dest_dict, value, None)
        except exc.UnmappedColumnError as err:
            _raise_col_to_prop(True, source_mapper, l, dest_mapper, r, err)

        # technically the "r.primary_key" check isn't
        # needed here, but we check for this condition to limit
        # how often this logic is invoked for memory/performance
        # reasons, since we only need this info for a primary key
        # destination.
        if (
            flag_cascaded_pks
            and l.primary_key
            and r.primary_key
            and r.references(l)
        ):
            uowcommit.attributes[("pk_cascaded", dest, r)] = True


def bulk_populate_inherit_keys(source_dict, source_mapper, synchronize_pairs):
    # a simplified version of populate() used by bulk insert mode
    for l, r in synchronize_pairs:
        try:
            prop = source_mapper._columntoproperty[l]
            value = source_dict[prop.key]
        except exc.UnmappedColumnError as err:
            _raise_col_to_prop(False, source_mapper, l, source_mapper, r, err)

        try:
            prop = source_mapper._columntoproperty[r]
            source_dict[prop.key] = value
        except exc.UnmappedColumnError:
            _raise_col_to_prop(True, source_mapper, l, source_mapper, r)


def clear(dest, dest_mapper, synchronize_pairs):
    for l, r in synchronize_pairs:
        if (
            r.primary_key
            and dest_mapper._get_state_attr_by_column(dest, dest.dict, r)
            not in orm_util._none_set
        ):

            raise AssertionError(
                "Dependency rule tried to blank-out primary key "
                "column '%s' on instance '%s'" % (r, orm_util.state_str(dest))
            )
        try:
            dest_mapper._set_state_attr_by_column(dest, dest.dict, r, None)
        except exc.UnmappedColumnError as err:
            _raise_col_to_prop(True, None, l, dest_mapper, r, err)


def update(source, source_mapper, dest, old_prefix, synchronize_pairs):
    for l, r in synchronize_pairs:
        try:
            oldvalue = source_mapper._get_committed_attr_by_column(
                source.obj(), l
            )
            value = source_mapper._get_state_attr_by_column(
                source, source.dict, l, passive=attributes.PASSIVE_OFF
            )
        except exc.UnmappedColumnError as err:
            _raise_col_to_prop(False, source_mapper, l, None, r, err)
        dest[r.key] = value
        dest[old_prefix + r.key] = oldvalue


def populate_dict(source, source_mapper, dict_, synchronize_pairs):
    for l, r in synchronize_pairs:
        try:
            value = source_mapper._get_state_attr_by_column(
                source, source.dict, l, passive=attributes.PASSIVE_OFF
            )
        except exc.UnmappedColumnError as err:
            _raise_col_to_prop(False, source_mapper, l, None, r, err)

        dict_[r.key] = value


def source_modified(uowcommit, source, source_mapper, synchronize_pairs):
    """return true if the source object has changes from an old to a
    new value on the given synchronize pairs

    """
    for l, r in synchronize_pairs:
        try:
            prop = source_mapper._columntoproperty[l]
        except exc.UnmappedColumnError as err:
            _raise_col_to_prop(False, source_mapper, l, None, r, err)
        history = uowcommit.get_attribute_history(
            source, prop.key, attributes.PASSIVE_NO_INITIALIZE
        )
        if bool(history.deleted):
            return True
    else:
        return False


def _raise_col_to_prop(
    isdest, source_mapper, source_column, dest_mapper, dest_column, err
):
    if isdest:
        util.raise_(
            exc.UnmappedColumnError(
                "Can't execute sync rule for "
                "destination column '%s'; mapper '%s' does not map "
                "this column.  Try using an explicit `foreign_keys` "
                "collection which does not include this column (or use "
                "a viewonly=True relation)." % (dest_column, dest_mapper)
            ),
            replace_context=err,
        )
    else:
        util.raise_(
            exc.UnmappedColumnError(
                "Can't execute sync rule for "
                "source column '%s'; mapper '%s' does not map this "
                "column.  Try using an explicit `foreign_keys` "
                "collection which does not include destination column "
                "'%s' (or use a viewonly=True relation)."
                % (source_column, source_mapper, dest_column)
            ),
            replace_context=err,
        )
