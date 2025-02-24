my_dict = {
    "i": 1,
    "ii": 2,
    "iii": 3
}

number = int(input("Введите число от 1 до 3: "))
if 1 <= int(number) <= 3:
    for key, val in my_dict.items():
        if val == number:
            if key:
                print(f"Ключ для числа {number}: {key}")
else:
    print("Пожалуйста, введите число в диапазоне от 1 до 3.")
