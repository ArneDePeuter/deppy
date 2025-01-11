from typing import Optional, Dict, Any, List


class Scope(dict):
    def __init__(self, parent: Optional[dict] = None) -> None:
        self.parent = parent
        self.children = []
        super().__init__()

    def __call__(self, key) -> List[Any]:
        values = []
        val = self.get(key)
        if val:
            values.append(val)
        for child in self.children:
            values.extend(child(key))
        return values

    def __getitem__(self, item) -> Any:
        val = self.get(item)
        if val is not None:
            return val
        if self.parent is not None:
            return self.parent[item]
        raise KeyError(item)

    def dump(self, str_keys=False) -> Dict[str, Any]:
        if str_keys:
            cp = {str(k): v for k, v in self.items()}
        else:
            cp = self.copy()
        if len(self.children) > 0:
            cp["children"] = [child.dump(str_keys=str_keys) for child in self.children]
        return cp

    def __str__(self) -> str:
        return str(self.dump())

    def birth(self) -> 'Scope':
        child = Scope(self)
        self.children.append(child)
        return child

    def __hash__(self) -> int:
        return id(self)
