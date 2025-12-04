# 🧩 C++ Projects Collection — *for_cv*

This repository contains a collection of **educational and portfolio-level C++ projects**, each demonstrating a core concept of modern C++ programming and computer systems design.

Every project is self-contained, built with **CMake**, and includes **GoogleTest**-based unit tests.  

Detailed descriptions, build instructions, and usage examples are located in the respective project folders.

---

## 🔹 Projects Overview
### [🤖 Traiding Bot for crypto exchange](./bot_binance)
Real-time market data processor for crypto exchange: using REST and WebSocket APIs with HMAC authentication, handling real-time market data and order lifecycle management. Demonstrates robust system design and network programming, integrating crypto exchange infrastructure.

---

### [📦 Parquet Data Auditor](./???)
C++17 CLI tool leveraging Apache Arrow and Zstd libraries to array integrity and ID continuity. Ensures structural correctness of time-series data for reliable analysis in trading platforms.

---

### [Strassen_matrix_production]
???

---

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

## 👤 Andrii Hisko – C++ Software Developer

📧 [andriihisko@gmail.com](mailto:andriihisko@gmail.com) || 📱 [\+380936728827](tel:+380936728827) ([WhatsApp](https://wa.me/380936728827)/[Telegram](https://t.me/x004147)/[Viber](https://chat?number=+380936728827)) || 💻 [GitHub](https://github.com/andpronew/for_cv) || 🔗 [LinkedIn](http://www.linkedin.com/in/andrii-hisko-6054b739b)

📍Odesa, Ukraine *(seeking remote role: home office, stable internet and power; immediately available for any-time work)* 

---

**🧾 SUMMARY**:

**C++ Software Developer** seeking a remote position with a company where I can apply my system-level developing skills. **Ph.D.**\-eduсated (**Theoretical Physics**) professional transitioning from a 25 year career in business management into a C++ systems programming role. Strong foundation in physics and mathematics combined with modern **C++17/20/23 skills**. Created several complex C++ codebases demonstrating expertise in **low-level programming, memory management, multithreading, and performance optimization**. This unique blend of technical prowess and business insight offers a distinctive perspective to engineering teams.  

---

**🛠️ SKILLS**:  
**🔧 Software Engineering Competencies:** ***Modern C++*** (17/20/23): STL, OOP, RAII, SOLID principles; smart pointers and ownership semantics; multithreading and concurrency (std::thread, mutex, condition\_variable, atomics); memory management and object lifetime; move semantics and copy elision; template programming; design patterns; resource-safe code and exception safety; low-level I/O, file descriptors, memory-mapped files (mmap); Linux system calls and signals; understanding of CPU cache, stack vs heap, memory alignment and padding; modular design and unit testing (GoogleTest) practices; etc. Strong understanding of ***computer architecture*** and ***operating systems*** fundamentals: CPU microarchitecture (ISA, assembly language (x86/MIPS/ARM)), instruction pipelines, RAM and cache behavior, memory alignment, virtual memory, system calls, process/thread scheduling, etc. and how these concepts influence C++ performance and design.

💻 **Systems & Tools:** Linux shell, API; GCC; Bash scripting; CMake, Ninja; Git/GitHub; VS Code, Vim; GoogleTest.

📊 **Algorithms & Data Structures:** sorting (quick, merge, heap, radix); searching (binary search, hash-based lookups); trees  (BST, AVL, B-trees, segment trees); graphs (BFS, DFS, Dijkstra, topological sort); hash tables and open addressing; stacks, queues, deques, priority queues; dynamic programming; bitwise manipulation and mask-based logic; time/space complexity analysis (Big-O); applied in time-series validation, binary protocol parsing, and numeric encoding tools; etc.

🌐 **Languages:** fluent English, native Russian and Ukrainian.  

---

**🔧 PROJECTS**: *All of these projects (and many others) are available on my GitHub [profile](https://github.com/andpronew/for_cv)*

**🤖 *Real-time market data processor for crypto exchange***: using REST and WebSocket APIs with HMAC authentication, handling real-time market data and order lifecycle management. Demonstrates robust system design and network programming, integrating crypto exchange infrastructure.  
**📦 *Parquet Data Auditor***:  
C++17 CLI tool leveraging Apache Arrow and Zstd libraries to array integrity and ID continuity. Ensures structural correctness of time-series data for reliable analysis in trading platforms.  
**🔒 *Custom Smart Pointers Library (Used in Finance Tools)***:  
Developed custom RAII-based unique\_ptr, shared\_ptr, and weak\_ptr with thread-safe reference counting and lifetime tracking for robust memory management. Integrated into memory-critical trading data modules to prevent memory leaks and ensure safe ownership semantics.

**🔧(In development**: directory scanner utility, shared memory/mmap tool, TCP server with listen socket, signal handler)

---

**🎓 EDUCATION**:

🧠 [**Ph.D. in Physics & Mathematics**](https://www.dissercat.com/content/dissipativnye-protsessy-i-nelineinye-ionno-zvukovye-vozmushcheniya-v-pylevoi-plazme) **(Theoretical Physics)** – [*Moscow Institute of Physics and Technology*](https://mipt.ru/) *(MIPT)*, 2006 (Master’s degree in physics and math, 2000): fundamental education in physics and mathematics: general and theoretical physics, mathematical analysis, numerical methods, etc. Advanced research in theoretical physics; published 16 scientific papers. Developed strong analytical and problem-solving skills dealing with complex systems and mathematical modeling.

💼 **M.B.A.** – *American Institute of Business and Economics* (Moscow), 2001: Graduate business education with focus on strategy, operations, and management. Enhanced skills in finance, leadership, marketing, operations and project management.

Ongoing **self-education** in C++, systems programming, and computer science fundamentals through advanced textbooks (*“Computer Organization and Design”* by Patterson & Hennessy, etc.) and online courses (computer science: Harvard, MIT, Carnegie Mellon; C++, Linux, bash, etc.). Regularly follow CppCon talks and ISO C++ updates to stay current with modern standards.

---

**🏢 EXPERIENCE – PRIOR MANAGEMENT CAREER (BRIEF)**:

***Nestlé (1997–2001):** Inventory Department Manager controlling all warehouses inventory.*

***Procter\&Gamble (2002–2003):** Supply Planning Manager coordinating production/supply from international plants.*

***Sveza (Severstal Group) (2003–2008):** Deputy General Director, IT Director, Logistics Director for one of major manufacturing holdings; led ERP-system rollout for \~200 users and managed IT infrastructure for \~500 users across 5 production sites (\~50 employees).*

*🇺🇦 **Ukrainian companies (2009–2022):*** 

*📦 Logistics Director managing perfume nationwide supply chain (\~50 employees).*

*🗂️ General Director of a document-management company (\~30 employees).*

*🚚 Launched and led last-mile courier service startup (\~30 employees).*

🔧 At each job I participated in **IT projects** to develop management systems and optimize work processes. This leadership background strengthened my **communication, project management, and analytical skills** — valuable assets in software engineering.

---

**💡 INTERESTS**:

Psychology, history, and systems design — curious about how **people** and **machines** work.
