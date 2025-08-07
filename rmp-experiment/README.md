# Rate My Professor API

This project demonstrates efficient ways to query the Rate My Professor API without overloading their GraphQL system, especially when dealing with multiple professors from the same university.

## The Problem

The naive approach (in `mvp.rs`) makes individual API calls for each professor using `get_teacher_summary_and_save()`. This becomes inefficient and may trigger rate limiting when you have:
- Multiple professors from the same university
- Large batches of queries
- Repeated queries to the same institutions

## The Solution

Use `get_professor_list()` to fetch ALL professors from a university in a single API call, then filter locally.

## Available Programs

### 1. Original MVP (`mvp.rs`)
```bash
cargo run --bin mvp "Queens College" "Ross Greenberg"
```
- Makes individual API calls per professor
- Good for single queries
- ❌ Inefficient for multiple professors from same university

### 2. Efficient Single University (`efficient_mvp.rs`)
```bash
cargo run --bin efficient "Queens College" "Ross Greenberg" "Rebecca Nelson"
```
- Single API call per university
- Local filtering for multiple professors
- ✅ Efficient for multiple professors from same university
- Includes partial name matching
- Set `SHOW_ALL=1` to see all available professors

### 3. Batch Processing (`batch_mvp.rs`)
```bash
cargo run --bin batch sample_queries.csv
```
- Processes CSV files with university,professor pairs
- Groups queries by university automatically
- Single API call per unique university
- ✅ Most efficient for large datasets
- Maintains input order in output
- Includes summary statistics

## Sample CSV Format

```csv
# Comments start with #
Queens College,Ross Greenberg
Queens College,Rebecca Nelson  
City College of New York,Douglas Troeger
Hunter College,Maria Rodriguez
```

## Key Benefits

1. **Reduced API Calls**: Instead of N calls for N professors, make 1 call per unique university
2. **Rate Limiting Avoidance**: Batch requests reduce server load
3. **Offline Filtering**: Fast local name matching after initial data fetch
4. **Fuzzy Matching**: Handles slight name variations
5. **Scalability**: Efficient for hundreds or thousands of queries

## API Efficiency Comparison

| Scenario | Original | Efficient | Batch |
|----------|----------|-----------|-------|
| 1 professor | 1 API call | 1 API call | 1 API call |
| 5 professors, same university | 5 API calls | 1 API call | 1 API call |
| 100 professors, 10 universities | 100 API calls | 10 API calls | 10 API calls |

## Performance Tips

1. **Group by University**: The batch processor automatically groups queries
2. **Respect Rate Limits**: Built-in 500ms delays between university queries
3. **Cache Results**: Consider saving `get_professor_list()` results locally for repeated use
4. **Name Normalization**: All matching is case-insensitive with whitespace handling

## Dependencies

```toml
rate_my_professor_api_rs = "0.1.5"
tokio = { version = "1", features = ["full"] }
anyhow = "1"
```

## Documentation References

- [Rate My Professor API Rust Crate](https://docs.rs/rate_my_professor_api_rs/0.1.5/rateMyProfessorApi_rs/methods/struct.RateMyProfessor.html)
- Key method: `get_professor_list()` returns `Vec<ProfessorList>` with department info
- Each `ProfessorList` contains: `first_name`, `last_name`, `department`, `avg_rating`, etc.

This approach scales from single queries to enterprise-level batch processing while being respectful to the Rate My Professor infrastructure. 