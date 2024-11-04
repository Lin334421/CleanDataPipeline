# 输入的文件后缀列表
file_extensions = [
    ".c", ".json", ".h", ".md", ".go", ".js", ".html", ".cpp", ".java", ".txt",
    ".rs", ".po", ".py", ".yml", ".xml", ".dmm", ".cc", ".ts", ".cs", ".ll",
    ".php", ".yaml", ".csv", ".result", ".hpp", ".patch", ".lock", ".svg",
    ".css", ".unity", ".s", ".mdpa", ".S", ".rst", ".sql", ".x", ".level",
    ".dtsi", ".dm", ".in", ".test", ".mdx", ".script_json", ".snap", ".rb",
    ".dts", ".tsx", ".inc", ".sh", ".pbtxt", ".ipynb", ".mm", ".ihex", ".obj",
    ".md", ".td", ".idf", ".defok", ".nix", ".cxx", ".kt", ".def", ".dat",
    ".markdown", ".out", ".svd", ".properties"
]

# 文件类型分类
categories = {
    "code": [
        ".c", ".cpp", ".h", ".go", ".js", ".py", ".rs", ".ts", ".php", ".cs",
        ".sh", ".rb", ".java", ".cc", ".tsx", ".kt", ".sql", ".s", ".S", ".cxx", ".mm",'.html','.hpp','.css'
    ],
    "config": [
        ".json", ".yml", ".yaml", ".xml", ".ini", ".toml", ".cfg", ".lock",
        ".dtsi", ".dts", ".pbtxt", ".nix"
    ],
    "docs": [
        ".md", ".txt", ".rst", ".mdx", ".csv", ".markdown", ".po"
    ],
    "other": [
        ".dmm", ".svg", ".unity", ".level", ".script_json", ".obj", ".ihex",
        ".idf", ".out", ".properties", ".patch", ".test", ".td", ".def",
        ".defok", ".dat", ".svd", ".snap", ".in"
    ]
}

# 创建一个结果字典来分类
categorized_files = {
    "code": [],
    "config": [],
    "docs": [],
    "other": []
}

# 遍历文件扩展名并分类
for ext in file_extensions:
    categorized = False
    for category, extensions in categories.items():
        if ext in extensions:
            categorized_files[category].append(ext)
            categorized = True
            break
    if not categorized:
        categorized_files["other"].append(ext)

# 打印分类结果
for category, files in categorized_files.items():
    print(f"{category.capitalize()} files: {files}")
