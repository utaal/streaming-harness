use std::ops::Add;

pub trait InputTimeResumableIterator<T: Eq+Ord>: Iterator<Item=T> {
    #[inline(always)]
    fn peek(&mut self) -> Option<&T>;
    #[inline(always)]
    fn end(&self) -> bool;
}

pub struct ConstantThroughputInputTimes<T: Copy+Eq+Ord+Add<DT, Output=T>, DT> {
    next: T,
    inter_arrival: DT,
    end: T,
}


impl<T: Copy+Eq+Ord+Add<DT, Output=T>, DT: Copy> ConstantThroughputInputTimes<T, DT> {
    pub fn new(first: T, inter_arrival: DT, end: T) -> Self {
        Self {
            next: first,
            inter_arrival,
            end,
        }
    }
}

impl<T: Copy+Eq+Ord+Add<DT, Output=T>, DT: Copy> Iterator for ConstantThroughputInputTimes<T, DT> {
    type Item = T;
    #[inline(always)]
    fn next(&mut self) -> Option<T> {
        if !self.end() {
            let n = self.next;
            self.next = self.next + self.inter_arrival;
            Some(n)
        } else {
            None
        }
    }
}

impl<T: Copy+Eq+Ord+Add<DT, Output=T>, DT: Copy> InputTimeResumableIterator<T> for ConstantThroughputInputTimes<T, DT> {
    #[inline(always)]
    fn peek(&mut self) -> Option<&T> {
        if !self.end() {
            Some(&self.next)
        } else {
            None
        }
    }
    #[inline(always)]
    fn end(&self) -> bool {
        self.next >= self.end
    }
}

pub struct SyntheticInputTimeGenerator<T: Copy+Eq+Ord, I: InputTimeResumableIterator<T>> {
    input_times: I,
    _phantom_data: ::std::marker::PhantomData<T>,
}

impl<T: Copy+Eq+Ord, I: InputTimeResumableIterator<T>> SyntheticInputTimeGenerator<T, I> {
    pub fn new(input_times: I) -> Self {
        Self {
            input_times,
            _phantom_data: ::std::marker::PhantomData,
        }
    }

    pub fn iter_until_incl<'a>(&'a mut self, until_incl: T) -> Option<impl Iterator<Item=T>+'a> {
        if self.input_times.end() {
            None
        } else {
            Some(SyntheticInputTimeGeneratorIterator {
                referenced: self,
                until_incl: until_incl,
            })
        }
    }
}

struct SyntheticInputTimeGeneratorIterator<'a, T: Copy+Eq+Ord+'a, I: InputTimeResumableIterator<T>+'a> {
    referenced: &'a mut SyntheticInputTimeGenerator<T, I>,
    until_incl: T,
}

impl<'a, T: Copy+Eq+Ord+'a, I: InputTimeResumableIterator<T>+'a> Iterator for SyntheticInputTimeGeneratorIterator<'a, T, I> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        if self.referenced.input_times.end() {
            None
        } else if let Some(&next_t) = self.referenced.input_times.peek() {
            if next_t <= self.until_incl {
                self.referenced.input_times.next().unwrap();
                Some(next_t)
            } else {
                None
            }
        } else {
            None
        }
    }
}
