"""
Fixed reference data for Northfield Institute of Technology (a fictional university).

Everything here is invented. Nothing comes from any real institution.
"""

# (dept_code, name)
DEPARTMENTS = [
    ("CSE", "Computer Science and Engineering"),
    ("ECE", "Electronics and Communication Engineering"),
    ("ME", "Mechanical Engineering"),
    ("CE", "Civil Engineering"),
    ("MGT", "School of Management"),
    ("SH", "Science and Humanities"),
]

# short code -> program details. intake = students admitted per year at scale 1.0
PROGRAMS = {
    "CSE": dict(code="BTECH-CSE", name="B.Tech Computer Science and Engineering", dept="CSE", level="UG", terms=8, intake=240, tuition=78000),
    "ECE": dict(code="BTECH-ECE", name="B.Tech Electronics and Communication Engineering", dept="ECE", level="UG", terms=8, intake=180, tuition=74000),
    "ME":  dict(code="BTECH-ME",  name="B.Tech Mechanical Engineering", dept="ME", level="UG", terms=8, intake=120, tuition=70000),
    "CIV": dict(code="BTECH-CE",  name="B.Tech Civil Engineering", dept="CE", level="UG", terms=8, intake=90, tuition=68000),
    "BCA": dict(code="BCA", name="Bachelor of Computer Applications", dept="CSE", level="UG", terms=6, intake=180, tuition=45000),
    "MCA": dict(code="MCA", name="Master of Computer Applications", dept="CSE", level="PG", terms=4, intake=90, tuition=55000),
    "MBA": dict(code="MBA", name="Master of Business Administration", dept="MGT", level="PG", terms=4, intake=150, tuition=90000),
    "BSM": dict(code="BSC-MATH", name="B.Sc Mathematics", dept="SH", level="UG", terms=6, intake=90, tuition=32000),
}

BTECH = ["CSE", "ECE", "ME", "CIV"]

# Semesters 1 and 2 are shared by all four B.Tech programs (one course, many programs:
# this is what the program_course bridge table exists for).
# Each entry: (title, course_type)
BTECH_COMMON = {
    1: [("Engineering Mathematics I", "theory"), ("Engineering Physics", "theory"),
        ("Programming in C", "theory"), ("English Communication", "theory"),
        ("Engineering Physics Lab", "lab")],
    2: [("Engineering Mathematics II", "theory"), ("Engineering Chemistry", "theory"),
        ("Basic Electrical Engineering", "theory"), ("Engineering Graphics", "theory"),
        ("Programming Lab", "lab")],
}

# Program-specific theory subjects, in semester order (4 per semester), plus one lab per semester.
THEORY = {
    "CSE": ["Data Structures", "Discrete Mathematics", "Digital Logic", "Object Oriented Programming",
            "Algorithms", "Database Systems", "Computer Organization", "Theory of Computation",
            "Operating Systems", "Computer Networks", "Software Engineering", "Compiler Design",
            "Machine Learning", "Distributed Systems", "Web Technologies", "Information Security",
            "Cloud Computing", "Data Engineering", "Natural Language Processing", "Big Data Analytics",
            "Blockchain Fundamentals", "Computer Vision", "Professional Ethics", "Project Management"],
    "ECE": ["Network Analysis", "Electronic Devices", "Signals and Systems", "Digital Electronics",
            "Analog Circuits", "Electromagnetic Theory", "Microprocessors", "Control Systems",
            "Communication Systems", "Linear Integrated Circuits", "Digital Signal Processing", "VLSI Design",
            "Antenna and Wave Propagation", "Embedded Systems", "Microwave Engineering", "Optical Communication",
            "Wireless Communication", "Internet of Things", "Satellite Communication", "Radar Systems",
            "Mobile Networks", "Image Processing", "Professional Ethics", "Project Management"],
    "ME":  ["Engineering Mechanics", "Thermodynamics", "Material Science", "Manufacturing Processes",
            "Fluid Mechanics", "Strength of Materials", "Kinematics of Machines", "Machine Drawing",
            "Heat Transfer", "Dynamics of Machines", "Design of Machine Elements", "Metrology",
            "IC Engines", "Refrigeration and Air Conditioning", "CAD CAM", "Industrial Engineering",
            "Finite Element Analysis", "Automobile Engineering", "Power Plant Engineering", "Robotics",
            "Operations Research", "Renewable Energy", "Professional Ethics", "Project Management"],
    "CIV": ["Surveying", "Building Materials", "Solid Mechanics", "Engineering Geology",
            "Structural Analysis I", "Fluid Mechanics for Civil", "Concrete Technology", "Transportation Engineering",
            "Structural Analysis II", "Geotechnical Engineering", "Hydrology", "Environmental Engineering",
            "Design of Steel Structures", "Design of Concrete Structures", "Water Resources Engineering", "Estimation and Costing",
            "Foundation Engineering", "Bridge Engineering", "Construction Management", "Town Planning",
            "Earthquake Engineering", "Pavement Design", "Professional Ethics", "Project Management"],
    "BCA": ["Fundamentals of Computers", "Mathematics for Computing", "Programming in Python", "Business Communication",
            "Data Structures using C", "Statistics", "Computer Architecture", "Accounting Basics",
            "Database Management", "Operating System Concepts", "Java Programming", "Software Engineering Basics",
            "Web Development", "Computer Networks Basics", "Data Analytics", "E-Commerce",
            "Cloud Fundamentals", "Mobile App Development", "Cyber Security Basics", "Information Systems",
            "Artificial Intelligence Basics", "Project Planning", "Entrepreneurship", "Professional Ethics"],
    "MCA": ["Advanced Data Structures", "Database Design", "Applied Mathematics", "Object Oriented Analysis",
            "Advanced Algorithms", "Enterprise Java", "Computer Networks and Security", "Data Warehousing",
            "Machine Learning Applications", "Cloud Architecture", "Big Data Systems", "DevOps Practices",
            "Deep Learning", "Distributed Databases", "Research Methodology", "IT Project Management"],
    "MBA": ["Principles of Management", "Managerial Economics", "Financial Accounting", "Organizational Behaviour",
            "Marketing Management", "Business Statistics", "Corporate Finance", "Human Resource Management",
            "Operations Management", "Business Research Methods", "Strategic Management", "Business Analytics",
            "Supply Chain Management", "Investment Analysis", "Digital Marketing", "Business Law",
            "International Business", "Entrepreneurship Development", "Corporate Governance", "Leadership and Change"],
    "BSM": ["Calculus", "Algebra", "Coordinate Geometry", "Physics for Mathematics",
            "Real Analysis", "Differential Equations", "Probability", "Computer Fundamentals",
            "Linear Algebra", "Numerical Methods", "Vector Calculus", "Mathematical Statistics",
            "Complex Analysis", "Abstract Algebra", "Discrete Structures", "Optimization Techniques",
            "Topology", "Number Theory", "Graph Theory", "Mathematical Modelling",
            "Functional Analysis", "Operations Research", "Financial Mathematics", "Research Project"],
}

LAB = {
    "CSE": "Programming Lab", "ECE": "Electronics Lab", "ME": "Workshop Lab", "CIV": "Survey and Materials Lab",
    "BCA": "Computer Lab", "MCA": "Software Lab", "BSM": "Computational Mathematics Lab",
}

# (code, name, is_refundable, amount_per_term, who_pays)
FEE_COMPONENTS = [
    ("TUITION", "Tuition Fee", False, None, "all"),          # amount comes from the program
    ("EXAM", "Examination Fee", False, 2500, "all"),
    ("LIBRARY", "Library Fee", False, 1500, "all"),
    ("HOSTEL", "Hostel Fee", True, 42000, "hostel"),
    ("TRANSPORT", "Transport Fee", True, 12000, "transport"),
]

# (city, state, weight)
CITIES = [
    ("Ranchi", "Jharkhand", 14), ("Jamshedpur", "Jharkhand", 8), ("Dhanbad", "Jharkhand", 6),
    ("Bokaro", "Jharkhand", 4), ("Patna", "Bihar", 9), ("Gaya", "Bihar", 3),
    ("Kolkata", "West Bengal", 8), ("Durgapur", "West Bengal", 2), ("Bhubaneswar", "Odisha", 4),
    ("Rourkela", "Odisha", 2), ("Raipur", "Chhattisgarh", 3), ("Lucknow", "Uttar Pradesh", 4),
    ("Varanasi", "Uttar Pradesh", 3), ("Delhi", "Delhi", 5), ("Bengaluru", "Karnataka", 3),
    ("Hyderabad", "Telangana", 3), ("Pune", "Maharashtra", 2), ("Bhopal", "Madhya Pradesh", 2),
    ("Guwahati", "Assam", 2), ("Siliguri", "West Bengal", 2),
]

DESIGNATIONS = [("Professor", 10), ("Associate Professor", 20), ("Assistant Professor", 55), ("Lecturer", 15)]

PAYMENT_MODES = [("upi", 55), ("card", 20), ("netbanking", 20), ("cash", 5)]

# Grade bands: (minimum percentage, grade)
GRADES = [(0.90, "O"), (0.80, "A+"), (0.70, "A"), (0.60, "B+"), (0.50, "B"), (0.45, "C"), (0.40, "P")]
