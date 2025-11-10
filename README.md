# 🧩 C++ Projects Collection — *for_cv*

This repository contains a collection of **educational and portfolio-level C++ projects**, each demonstrating a core concept of modern C++ programming and computer systems design.  
Every project is self-contained, built with **CMake**, and includes **GoogleTest**-based unit tests.  
Detailed descriptions, build instructions, and usage examples are located in the respective project folders.

---

## 🔹 Projects Overview

### [🧮 Binary Operations Library (C++)](./binary_operations)
A simple C++ project that demonstrates **manual binary addition and subtraction** using **two’s complement arithmetic** — exactly how real CPU ALUs perform it.  
Focus: bitwise logic, carry propagation, and hardware-level arithmetic emulation.

---

### [🔢 Base Converter Project](./base_converter)
**Goal:** Convert numbers between arbitrary bases *(2 ≤ b ≤ 16)*, with digits above 9 represented by letters **A–F**.  
**Skills learned:** Deep understanding of **base conversion**, character–digit mapping, and string-based arithmetic. Tested via **GoogleTest**, built with **CMake**.

---

### [🧠 Smart Pointers Library](./smart_ptr_project)
An educational re-implementation of **C++ smart pointers**, showing understanding of **memory management**, **RAII**, and **ownership semantics**.  
Implements:
- `UniquePtr` — exclusive ownership (like `std::unique_ptr`)  
- `SharedPtr` — reference-counted shared ownership (like `std::shared_ptr`)  
- `WeakPtr` — non-owning observer (like `std::weak_ptr`)  

Includes copy/move semantics, destructors, and basic thread-safe reference counting.

---

## ⚙️ Build System and Tests

Each project:
- Uses **CMake** for portable builds (`mkdir build && cd build && cmake .. && make`)
- Includes **unit tests** via **GoogleTest**
- Targets **C++17 or higher**

---

## 📘 Purpose

This repository serves as a **demonstration of C++ programming proficiency**, focusing on:
- Low-level systems concepts  
- Safe memory management  
- Clear modular project design  
- Clean CMake and testing integration  

---

## 🧩 Author

**Andrii Hisko** — C++ developer (PhD in physics) 
Exploring the intersection of **computer architecture**, **systems programming**, and **mathematical algorithms** through practical C++ projects.
