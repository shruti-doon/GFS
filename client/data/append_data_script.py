for i in range(10):
    filename = f"appedata_client_{i+1}.txt"
    content = chr(ord('a') + i) * 100  # Generates 'a'*100, 'b'*100, ..., 'j'*100
    with open(filename, "w") as file:
        file.write(content)
