Heuristic 1:
Heuristic 1: All data should be hidden within its class.
Explanation: This heuristic means all data attributes should be private. In lecture, this was presented as one of the foundational rules behind most coding standards. If data is exposed publicly, external classes can change it directly, which breaks encapsulation and makes bugs harder to trace. Hiding data forces all changes to go through the class's own methods, making the design easier to maintain.
Heuristic 2:
A class should capture one and only one key abstraction
A class that mixes multiple abstractions becomes hard to understand and change without breaking something unrelated. In lecture, we used CRC cards to design classes, where each card represents a single class with its responsibilities and collaborators. This process naturally enforces H2.8 because if a class ends up with too many unrelated responsibilities on its card, that is a sign it needs to be split into separate classes, each with one clear abstraction.
Heuristic 3:
Do not create god classes/objects in your system
Explanation: A god class is one that controls too much of the system, either holding too much data or too much behavior. In lecture, this connected to the MVC pattern, where the Model, View, and Controller each have a distinct responsibility. Combining them into one class would create a god class that handles data, display, and user input all at once. Keeping them separate distributes responsibility properly and avoids the exact problem H3.2 warns against.