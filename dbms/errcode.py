class UnrecognizedLoginError(Exception):
    def __str__(self):
        return "ID PW를 입력해주세요"
