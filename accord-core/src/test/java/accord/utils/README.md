# Fuzz Testing

There are multiple ways to do fuzz testing, and this package provides different utilities to aid in writing tests.

# Gen

# Property-Based Testing with `qt()`

Property-based testing generates random test inputs to validate that certain properties or invariants of your code hold true regardless of the input values. The `Property.qt()` method enables this style of testing, similar to libraries like QuickCheck or QuickTheory.

## Basic Usage

```java
import static accord.utils.Property.qt;

// Run a simple property test
qt().check(random -> {
    // Test with random values
    int value = random.nextInt();
    assert someInvariant(value);
});
```

```java
import static accord.utils.Property.qt;
qt().forAll(Gens.strings())
    .check(str -> {
        // Property assertion goes here
    });
```

## Main Features

### Core Method: `qt()`
Returns a `ForBuilder` instance that serves as the starting point for defining a property test.

### Configuration Methods

- **withSeed(long seed)**: Sets a specific random seed for reproducible tests
- **withExamples(int count)**: Sets the number of test cases to generate (default is 1000)
- **withPure(boolean pure)**: Controls whether to use a fresh random seed for each example (default is true)
- **withTimeout(Duration timeout)**: Sets a timeout for test execution

### Test Input Generation

- **forAll(Gen<T> gen)**: Single generator for one parameter
- **forAll(Gen<A> a, Gen<B> b)**: Two generators for two parameters
- **forAll(Gen<A> a, Gen<B> b, Gen<C> c)**: Three generators for three parameters

### Execution

- **check(Consumer<T> fn)**: Runs the test with the specified property function

## Error Handling

When a property test fails, the framework creates a detailed error report including:

- The random seed used (for test reproducibility)
- Number of examples planned
- Whether pure mode was enabled
- The error message and exception
- The generated values that caused the failure
