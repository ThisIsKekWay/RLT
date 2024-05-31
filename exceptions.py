class ParsingFailedException(Exception):
    message = "Невалидные данные, напишите '/help' для инструкций"


class WrongDateException(Exception):
    message = "Данный диапазон не найден, уточните дату."