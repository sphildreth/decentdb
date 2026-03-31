using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace DecentDb.ShowCase.Entities;

public class Order
{
    [Key]
    public long Id { get; set; }

    [Required]
    [MaxLength(50)]
    public string OrderNumber { get; set; } = string.Empty;

    public DateTime OrderDate { get; set; }

    [Column(TypeName = "DECIMAL(18,2)")]
    public decimal TotalAmount { get; set; }

    public bool IsShipped { get; set; }

    public DateTime? ShippedAt { get; set; }

    public OrderStatus Status { get; set; } = OrderStatus.Pending;

    public long CustomerId { get; set; }
    [ForeignKey(nameof(CustomerId))]
    public Customer? Customer { get; set; }

    public long? ShippingAddressId { get; set; }
    [ForeignKey(nameof(ShippingAddressId))]
    public Address? ShippingAddress { get; set; }

    public DateTime CreatedAt { get; set; }

    public ICollection<OrderItem> OrderItems { get; set; } = new List<OrderItem>();
}

public enum OrderStatus
{
    Pending = 0,
    Processing = 1,
    Shipped = 2,
    Delivered = 3,
    Cancelled = 4
}
