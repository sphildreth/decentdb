using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace DecentDb.ShowCase.Entities;

public class OrderItem
{
    [Key]
    public long Id { get; set; }

    public long OrderId { get; set; }

    public long ProductId { get; set; }

    [Column(TypeName = "DECIMAL(18,4)")]
    public decimal UnitPrice { get; set; }

    public int Quantity { get; set; }

    [Column(TypeName = "DECIMAL(18,4)")]
    public decimal Discount { get; set; }

    public DateTime CreatedAt { get; set; }
}
