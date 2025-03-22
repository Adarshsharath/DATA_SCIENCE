class Employee:
    def __init__(self):
        self.dict = {}  

    def add_employee(self, id, name, age, department, salary):
        if id not in self.dict:
            self.dict[id] = {"Name": name, "Age": age, "Department": department, "Salary": salary}
            print(f"Employee {id} - {name} added successfully")
        else:
            print(f"Employee with ID {id} already exists.")

    def view_employee(self):
        if not self.dict:
            print("No employees available.")
            return

        print(f"{'ID':<5} {'Name':<10} {'Age':<5} {'Department':<15} {'Salary':<10}")
        print("-" * 50)

        for emp_id, info in self.dict.items():
            print(f"{emp_id:<5} {info['Name']:<10} {info['Age']:<5} {info['Department']:<15} {info['Salary']:<10}")

    def search_employee(self):
        emp_id = int(input("Enter Employee ID to search: "))

        if emp_id in self.dict:
            info = self.dict[emp_id]
            print("\nEmployee Found:")
            print(f"ID: {emp_id}")
            print(f"Name: {info['Name']}")
            print(f"Age: {info['Age']}")
            print(f"Department: {info['Department']}")
            print(f"Salary: {info['Salary']}")
        else:
            print("Employee not found.")

emp = Employee()

while True:
    print("""
          1. Add Employee
          2. View Employees
          3. Search Employee
          4. Exit
          """)
    
    choice = int(input("Enter your choice: "))

    if choice == 1:
        id = int(input("Enter Employee ID: "))
        name = input("Enter Employee Name: ")
        age = int(input("Enter Employee Age: "))
        department = input("Enter Employee Department: ")
        salary = float(input("Enter Employee Salary: "))
        emp.add_employee(id, name, age, department, salary)
    elif choice == 2:
        emp.view_employee()
    elif choice == 3:
        emp.search_employee()
    elif choice == 4:
        print("Thank you for using the Employee Management System....")
        print("Exiting...")
        break
    else:
        print("Invalid choice! Please try again.")
