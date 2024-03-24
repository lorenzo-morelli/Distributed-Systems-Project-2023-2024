

id = int(input("Enter the number of the program: "))
key = int(input("Enter the number of the key: "))

for i in range(1, key):
    print(f'"files/program{id}/key{i}.csv",')
print(f'"files/program{id}/key{key}.csv"')

