class Mission:
    """Contains basic mission-wide configuration used by member datasets"""
    id: str
    label: str

class GraceFO(Mission):
    id = 'GRACEFO'
    label = 'GRACE-FO'
