using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace DecentDb.ShowCase.Entities;

public class Customer
{
    [Key]
    public long Id { get; set; }

    [Required]
    [MaxLength(100)]
    public string FirstName { get; set; } = string.Empty;

    [Required]
    [MaxLength(100)]
    public string LastName { get; set; } = string.Empty;

    [MaxLength(255)]
    public string? Email { get; set; }

    [MaxLength(20)]
    public string? Phone { get; set; }

    public bool IsPremium { get; set; }

    public DateTime? LastPurchaseDate { get; set; }

    public decimal? TotalSpend { get; set; }

    public int LoyaltyPoints { get; set; }

    public Guid? PreferredStoreId { get; set; }

    public DateTime CreatedAt { get; set; }

    public DateTime? UpdatedAt { get; set; }

    public ICollection<Order> Orders { get; set; } = new List<Order>();
}
