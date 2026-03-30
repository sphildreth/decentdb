using System.ComponentModel.DataAnnotations;

namespace DecentDb.ShowCase.Entities;

public class WarehouseLocation
{
    [MaxLength(32)]
    public string WarehouseCode { get; set; } = string.Empty;

    [MaxLength(32)]
    public string BinCode { get; set; } = string.Empty;

    [Required]
    [MaxLength(100)]
    public string Zone { get; set; } = string.Empty;

    public bool TemperatureControlled { get; set; }

    public ICollection<InventoryCount> InventoryCounts { get; set; } = new List<InventoryCount>();
}
