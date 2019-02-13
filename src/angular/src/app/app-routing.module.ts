import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import {ProductSearchComponent} from "./product-search/product-search.component";
import {ProductComponent} from "./product/product.component";
import {AppComponent} from "./app.component";

const routes: Routes = [
  {path: '', component: ProductSearchComponent},
  {path: 'product/:productId', component: ProductComponent}
];


@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule {}
