# Proposed Redesign CRC Cards

The system is redesigned by breaking the original OrderProcessor into smaller classes, each with a clear responsibility.

Class: Order

Responsibilities:
Store customer name, email, item, and price  
Provide access to order data through getters

Collaborators:
None

Class: PricingService

Responsibilities:
Calculate tax  
Apply discounts  
Return the final total

Collaborators:
Order

Class: OrderRepository

Responsibilities:
Save order data to a file or database  
Hide all file handling details from the rest of the system

Collaborators:
Order

Class: NotificationService

Responsibilities:
Send confirmation emails

Collaborators:
Order

Class: OrderProcessor

Responsibilities:
Control the overall process  
Call PricingService for calculations  
Call OrderRepository to save data  
Call NotificationService to send emails  
Print the final receipt

Collaborators:
Order  
PricingService  
OrderRepository  
NotificationService  