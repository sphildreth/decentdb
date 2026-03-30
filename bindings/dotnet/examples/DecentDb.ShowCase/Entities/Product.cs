using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace DecentDb.ShowCase.Entities;

public class Product
{
    [Key]
    public long Id { get; set; }

    [Required]
    [MaxLength(200)]
    public string Name { get; set; } = string.Empty;

    [MaxLength(2000)]
    public string? Description { get; set; }

    [Column(TypeName = "DECIMAL(18,4)")]
    public decimal Price { get; set; }

    public int StockQuantity { get; set; }

    public bool IsActive { get; set; } = true;

    [Column(TypeName = "DECIMAL(10,2)")]
    public decimal? Weight { get; set; }

    public DateTime CreatedAt { get; set; }

    public DateTime? UpdatedAt { get; set; }

    public DateTimeOffset? DiscontinuedAt { get; set; }

    public byte[]? ImageData { get; set; }

    public Guid? Sku { get; set; }

    public TimeSpan? ShelfLife { get; set; }

    public long CategoryId { get; set; }
    [ForeignKey(nameof(CategoryId))]
    public Category? Category { get; set; }

    public ICollection<OrderItem> OrderItems { get; set; } = new List<OrderItem>();
    public ICollection<ProductTag> ProductTags { get; set; } = new List<ProductTag>();

    public int Version { get; set; }
}
