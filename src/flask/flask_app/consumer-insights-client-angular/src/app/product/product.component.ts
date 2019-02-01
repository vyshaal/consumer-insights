import { Component, OnInit } from '@angular/core';
import {ActivatedRoute} from "@angular/router";
import {Product} from "../../models/product.model.client";
import {ProductService} from "../../services/product.service.client";
import {Review} from "../../models/review.model.client";
import {ReviewService} from "../../services/review.service.client";

@Component({
  selector: 'app-product',
  templateUrl: './product.component.html',
  styleUrls: ['./product.component.css']
})
export class ProductComponent implements OnInit {

  constructor(private activatedRoute: ActivatedRoute, private productService: ProductService,
              private reviewService: ReviewService) {}

  product: Product;
  reviews: Review[];
  dummy = [];
  product_id = "";

  ngOnInit() {
    this.activatedRoute.paramMap.subscribe(params => {
      console.log(params.get('productId'));
      this.productService.findProductById(params.get('productId')).then(response => this.product = response);
    });

  }

}
