using DecentDb.ShowCase.Entities;

namespace DecentDb.ShowCase;

internal static class ShowcaseSeeder
{
    public static Category CreateCategory(
        string name,
        int displayOrder,
        string? description = null,
        DateOnly? effectiveFrom = null,
        TimeOnly? businessHoursStart = null,
        bool isVisible = true)
    {
        return new Category
        {
            Name = name,
            Description = description,
            EffectiveFrom = effectiveFrom ?? DateOnly.FromDateTime(DateTime.UtcNow),
            BusinessHoursStart = businessHoursStart ?? new TimeOnly(9, 0),
            DisplayOrder = displayOrder,
            IsVisible = isVisible,
        };
    }

    public static Product CreateProduct(
        string name,
        decimal price,
        int stockQuantity,
        long categoryId,
        string? description = null,
        DateTime? createdAt = null,
        Guid? sku = null,
        decimal? weight = null,
        bool isActive = true)
    {
        return new Product
        {
            Name = name,
            Description = description,
            Price = price,
            StockQuantity = stockQuantity,
            CategoryId = categoryId,
            CreatedAt = createdAt ?? DateTime.UtcNow,
            Sku = sku,
            Weight = weight,
            IsActive = isActive,
        };
    }

    public static Customer CreateCustomer(
        string firstName,
        string lastName,
        string? email = null,
        DateTime? createdAt = null)
    {
        return new Customer
        {
            FirstName = firstName,
            LastName = lastName,
            Email = email,
            CreatedAt = createdAt ?? DateTime.UtcNow,
        };
    }

    public static Order CreateOrder(
        string orderNumber,
        long customerId,
        decimal totalAmount,
        DateTime? orderDate = null,
        OrderStatus status = OrderStatus.Pending,
        DateTime? createdAt = null,
        long? shippingAddressId = null)
    {
        return new Order
        {
            OrderNumber = orderNumber,
            CustomerId = customerId,
            OrderDate = orderDate ?? DateTime.UtcNow,
            TotalAmount = totalAmount,
            Status = status,
            ShippingAddressId = shippingAddressId,
            CreatedAt = createdAt ?? DateTime.UtcNow,
        };
    }

    public static OrderItem CreateOrderItem(
        long orderId,
        long productId,
        decimal unitPrice,
        int quantity,
        decimal discount = 0m,
        DateTime? createdAt = null)
    {
        return new OrderItem
        {
            OrderId = orderId,
            ProductId = productId,
            UnitPrice = unitPrice,
            Quantity = quantity,
            Discount = discount,
            CreatedAt = createdAt ?? DateTime.UtcNow,
        };
    }

    public static Tag CreateTag(
        string name,
        bool isSystem = false,
        int usageCount = 0,
        string? description = null,
        DateTime? createdAt = null)
    {
        return new Tag
        {
            Name = name,
            IsSystem = isSystem,
            UsageCount = usageCount,
            Description = description,
            CreatedAt = createdAt ?? DateTime.UtcNow,
        };
    }
}
