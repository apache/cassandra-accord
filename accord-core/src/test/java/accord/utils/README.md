# Property Testing

There are multiple ways to do property testing, and this package provides different utilities to aid in writing tests.

# Gen

The `Gen` class is the core abstraction for generating random test values in our property-based testing framework. A `Gen<T>` instance defines how to produce random values of type `T` using a provided `RandomSource`.

## Core Concepts

### The `Gen` Interface

`Gen<T>` is a functional interface with a single method:

```java
T generate(RandomSource random);
```

This method produces a random value of type `T` using the given random source.

### Using `Gens` Utility Class

The `Gens` class provides a comprehensive set of pre-defined generators for common types:

#### Primitive Types
- `Gens.bools()`: Generates random boolean values based off a pattern
- `Gens.ints()`, `Gens.longs()`: Generates random integer/long values based off a pattern
- `Gens.enum()`: Generates enum values based off a pattern
- `Gens.strings()`: Generates strings based off a pattern

#### Collection Generators
- `Gens.lists(itemGen)`, `Gens.arrays(class, itemGen)`: Lists/arrays of random items based off a pattern

#### Advanced Generators
- `Gens.oneOf(T... values)`: Selects randomly from provided values
- `Gens.constant(value)`: Always generates the same value

#### Meta-Randomness Generators

To put it simply: a generator of generators.  These generators define how to generate randomness itself!

- `Gens.mixedDistribution`: This is a group of functions that will select values from some range or input, and the selection will be based off a distribution.  When working with these functions the top level `Gen` selects what distribution to use, and will return a `Gen` that selects values from that distribution.

### Composing Generators

Generators can be transformed and combined using methods like:

```java
// Map a generator to a different type
Gen<String> stringGen = Gens.ints().between(0, 10).map(i -> "Number: " + i);

// Filter generated values
Gen<Integer> evenNumbers = Gens.ints().between(0, 10).filter(i -> i % 2 == 0);
```

### Example Usage

```java
// Generate a random person object
Gen<String> nameGen = Gens.strings().all().ofLengthBetween(1, 10);
Gen.IntGen ageGen = Gens.ints().between(18, 100) // IntGen exposes a "nextInt" function without boxing
Gen<Person> personGen = rs -> new Person(nameGen.next(rs), ageGen.nextInt(rs));

// Create a more complex generator with bias
Gen<Person> complexPersonGen = Gens.oneOf(Map.of(
    youngPersonGen, 3, // 3x weight for young people
    adultPersonGen, 2, // 2x weight for adults
    seniorPersonGen, 1 // 1x weight for seniors
));

enum Level {IC1, IC2, IC3, IC4, IC5, IC6, IC7}
// Use "meta-randomness" to have your test change the distribution
Gen<Gen<Level> levelDistribution = Gens.enums().allMixedDistribution(Level.class);
// Select the actual distribution for the test
Gen<Level> levelGen = levelDistribution.next(rs);
```

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

// Run a property test with strings as input
qt().forAll(Gens.strings().ofLengthBetween(0, 100))
    .check(str -> {
        // Property assertion goes here
    });
```

## Terms

- `seed` - input to a `RandomSource` for reproducible tests
- `example` - a single test execution
- `pure` - does the test have side effects (that might impact the reproducibility)?

## Main Features

### Core Method: `qt()`
Returns a `ForBuilder` instance that serves as the starting point for defining a property test.

### Configuration Methods

- `withSeed(long seed)`: Sets a specific random seed for the test (useful when retrying failed tests)
- `withExamples(int count)`: Sets the number of test cases to generate (default is 1000)
- `withPure(boolean pure)`: Controls whether to use a fresh random seed for each example (default is true)
- `withTimeout(Duration timeout)`: Sets a timeout for test execution

### Test Input Generation

- `forAll(Gen<T> gen)`: Single generator for one parameter
- `forAll(Gen<A> a, Gen<B> b)`: Two generators for two parameters
- `forAll(Gen<A> a, Gen<B> b, Gen<C> c)`: Three generators for three parameters

### Execution

- `check(Consumer<T> fn)`: Runs the test with the specified property function

## Error Handling

When a property test fails, the framework creates a detailed error report including:

- The random seed used (for test reproducibility)
- Number of examples planned
- Whether pure mode was enabled
- The error message and exception
- The generated values that caused the failure

# Stateful Property-Based Testing with `stateful()`

Stateful property testing allows you to verify that your system behaves correctly through a sequence of operations. Unlike simple property testing which validates individual functions, stateful testing models interactions with a stateful system and verifies that invariants hold throughout operation sequences.

## Basic Usage

```java
import static accord.utils.Property.stateful;
import static accord.utils.Property.commands;

stateful().check(commands(() -> State::new)
		.add(create()) // add "create" command with random weight
		.add(read())   // add "read" command with random weight
		.add(update()) // add "update" command with random weight
		.add(delete()) // add "delete" command with random weight
        .build());
```

## Terms

`stateful` inherits same terms from `qt`, but adds the following

- `State` - state needed by the test to track changes (eg. a model), and/or utility functions
- `SystemUnderTest` - the thing that is being tested
- `command` - a single operation to apply to both the `State` and the `SystemUnderTest`
- `step` - one `Command` to execute

For some tests, `SystemUnderTest` is not needed, in which case `State` acts as both `State` and `SystemUnderTest`.

## Basic Concepts

### Core Method: `stateful()`

The `Property.stateful()` method is the entry point for stateful property testing, similar to `qt()` for regular property testing, but takes a `Commands` rather than a lambda; this `Commands` defines what is allowed to happen in the test.

## Configuration Options

The same configuration options exist as `qt`, but also add the following

- `withSteps(int)`: Maximum number of commands to try per test run (default is 1000)
- `withStepTimeout(Duration)`: - Max amount of time a single `step` can take.  This method differs from `withTimeout` as it focuses on a single `step` where as `withTimeout` focuses on the `example`.

### Command

Command consist of three parts:
1. **Execution**: Apply the `Command` to both the `State` and `SystemUnderTest`
2. **Verification**: Check that `State` and `SystemUnderTest` maintain a given set of properties
3. **Display**: Creates a human readable string about what this Command does

A typical command looks like this:

```java
class AddItemCommand implements UnitCommand<ShoppingCart, ActualCart> 
{
    private final Item item;
    
    public AddItemCommand(Item item) 
    {
        this.item = item;
    }
    
    // Update the model (what we expect to happen)
    @Override
    public void applyUnit(ShoppingCart model) 
    {
        model.add(item);
    }
    
    // Apply to the real system
    @Override
    public void runUnit(ActualCart sut) 
    {
        sut.addItem(item);
    }
    
    // Verify that model and sut match after command execution
    @Override
    public void checkPostconditions(ShoppingCart model, ActualCart sut) 
    {
        assertEquals(model.getItems(), sut.getItems());
    }
    
    // For history logging
    @Override
    public String detailed(ShoppingCart model) 
    {
        return "Add item: " + item;
    }
}
```

But it is common that the test doesn't include a `SystemUnderTest`, in which case the command would look like

```java
class AddItemCommand implements StateOnlyCommand<ShoppingCart>
{
    private final Item item;
    
    // Constructor for the command with its parameters
    public AddItemCommand(Item item) 
    {
        this.item = item;
    }
    
    // Update the model (what we expect to happen)
    @Override
    public void applyUnit(ShoppingCart model) 
    {
        model.add(item);
        // verify anything needed
    }

    // For history logging
    @Override
    public String detailed(ShoppingCart model) 
    {
        return "Add item: " + item;
    }
}
```

Or can use `SimpleCommand` instead

```java
Item item = ...
new SimpleCommand<>("Add item: " + item, state -> state.add(item));
```

### Commands

`Commands` represent a set of `Command` that can be performed on both the `State` and `SystemUnderTest`.

- `genInitialState` - called at the start of each "example", and creates a new `State`
- `createSut(state)` - called at the start of each "example", and creates a new `SystemUnderTest` from the provided `State`
- `destroyState(state, @Nullable cause)`, `destroySut(sut, @Nullable cause)` - called at the end of each "example" to allow closing resources
- `commands(state)` - called at each "attempt" to create a generator of `Command`.

## Commands Builder

The framework provides a `Commands` builder to simplify `Commands` generation:

```java
commands(() -> State::new, Sut::new)
// Add command with fixed weight
.add(3, (rs, state) -> new AddItemCommand(Gens.items().next()))
// Add command with random weights! At the start of each example, the weight is computed; each example will run with different weights
// Should default to this method over explicit weights
.add((rs, state) -> new RemoveItemCommand())
// Add conditional command
.addIf(state -> !state.isEmpty(),
	   (rs, state) -> new CheckoutCommand())
.build();
```

## Error Reporting

When a stateful test fails, the framework provides detailed information:
- The command sequence that led to the failure
- The random seed for reproducibility
- The specific command that failed
- The exception and error message

## Example: Testing a Queue Implementation

```java
stateful()
    .check(commands(() -> MyQueueImplementation::<>new)
        .add((rs, state) -> new EnqueueCommand<>(Gens.ints().all().next(rs)))
        .addIf(state -> !state.isEmpty(),
              (rs, state) -> new DequeueCommand<>())
        .addIf(state -> !state.isEmpty(),
              (rs, state) -> new PeekCommand<>())
        .build());
```

This will generate and run sequences of queue operations, automatically checking that your implementation behaves correctly.
