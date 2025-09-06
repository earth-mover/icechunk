def dataclass_repr(
    obj: object,
    # make this default to inspecting the cls?
    cls_name: str,
    attributes: list[str] = None,
    # TODO optional indent
) -> str:
    """
    Dynamically create a repr for this dataclass-like object.

    Parameters
    ----------
    obj : object
        Object for which to make a repr.
    cls_name : Type
        What to display as the name of the class, including submodule.
    attributes : list[str] | None
        Names of attributes or properties to display the values of. 
        These must all exist on the instance and be printable.

    Returns
    -------
    str
        Repr for the class.
    """
    header = f"<{cls_name}>"

    if not attributes:
        return header
    else:
        contents = []
        for attr_name in attributes:
            line = f"{attr_name}: {getattr(obj, attr_name)}"
            contents.append(line)
        return "\n".join([header] + contents)
