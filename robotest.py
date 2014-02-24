
class RoboTest:

    def __init__(self, magic):
        self.__magic = magic

    def is_magic(self, magic):
        assert self.__magic == magic, "That wasn't right"

