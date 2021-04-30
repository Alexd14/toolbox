class SliceHolder:
    """
    holds information on the start and end indexes for a slice.
    assumes start and end are immutable references
    """

    def __init__(self, start, end):
        self.__start = start
        self.__end = end

    @property
    def start(self):
        return self.__start

    @property
    def end(self):
        return self.__end

    def __str__(self):
        return str(self.__start) + ', ' + str(self.__end)

    def __repr__(self):
        return self.__str__()
