class DatasetConfig:
    """
    Container for information about a dataset.

    Attributes:
        name: The name of the dataset.
        path: The path to the dataset.
        key: The key of the TTree in the ROOT file.
        year: The year of the dataset
        is_mc: Is the dataset MC or not
        xsec: The cross section of the dataset
        partitions: number of partitions when building the dataset
        stepsize: step size to use in the dataset preprocessing
        filenames: Filenames of the ROOT files.
    """

    def __init__(
        self,
        name: str,
        path: str,
        key: str,
        year: str,
        is_mc: bool,
        xsec: float,
        partitions: int,
        stepsize: int,
        filenames: tuple,
    ) -> None:
        if path[-1] != "/":
            raise ValueError(f"Dataset path has to end with '/'. Got: {path}")

        self.name = name
        self.path = path
        self.key = key
        self.year = year
        self.is_mc = is_mc
        self.xsec = xsec
        self.partitions = partitions
        self.stepsize = stepsize
        self.filenames = filenames

    def __repr__(self):
        return f"DatasetConfig({self.name}, {self.year}, {self.stepsize})"