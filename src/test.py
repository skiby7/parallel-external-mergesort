def is_sorted_with_mismatch(numbers):
    """Check if a list of numbers is sorted and return the first mismatch position."""
    for i in range(len(numbers) - 1):
        if numbers[i] > numbers[i + 1]:
            return False, i + 1  # Return 1-based index for user readability
    return True, None

def check_sorted_file(filename):
    """Read file with one integer per line and check if the sequence is sorted."""
    try:
        numbers = []
        line_numbers = []  # Track which line each number came from

        with open(filename, 'r') as file:
            for line_num, line in enumerate(file, 1):
                line = line.strip()

                # Skip empty lines
                if not line:
                    continue

                try:
                    # Parse single integer from the line
                    number = int(line)
                    numbers.append(number)
                    line_numbers.append(line_num)

                except ValueError as e:
                    print(f"Line {line_num}: Error parsing integer '{line}' - {e}")
                    return

        # Check if the entire sequence is sorted
        if len(numbers) == 0:
            print("File is empty or contains no valid integers.")
        elif len(numbers) == 1:
            print(f"Single number: {numbers[0]} - Trivially sorted")
        else:
            is_sorted, mismatch_pos = is_sorted_with_mismatch(numbers)
            if is_sorted:
                print(f"Sequence SORTED")
            else:
                # Find the actual line numbers for the mismatch
                mismatch_line = line_numbers[mismatch_pos]
                prev_line = line_numbers[mismatch_pos - 1]
                print(f"Sequence NOT SORTED")
                print(f"Mismatch found: {numbers[mismatch_pos - 1]} (line {prev_line}) > {numbers[mismatch_pos]} (line {mismatch_line})")

    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
    except Exception as e:
        print(f"Error reading file: {e}")

# Example usage
if __name__ == "__main__":
    # You can change this to your file path
    filename = "out"
    check_sorted_file("out")
    check_sorted_file("out1")
    check_sorted_file("out2")

    # Alternative: Ask user for filename
    # filename = input("Enter filename: ")
    # check_sorted_file(filename)
