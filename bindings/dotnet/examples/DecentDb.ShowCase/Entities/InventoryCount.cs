using System.ComponentModel.DataAnnotations;

namespace DecentDb.ShowCase.Entities;

public class InventoryCount
{
    [Key]
    public long Id { get; set; }

    [MaxLength(32)]
    public string WarehouseCode { get; set; } = string.Empty;

    [MaxLength(32)]
    public string BinCode { get; set; } = string.Empty;

    [Required]
    [MaxLength(100)]
    public string ProductName { get; set; } = string.Empty;

    public int QuantityOnHand { get; set; }

    public DateTime CountedAt { get; set; }

    public WarehouseLocation? Location { get; set; }
}
